package benchmarks.common

import java.util.concurrent.LinkedBlockingQueue
import java.util.{Locale, Properties, Random}

import joptsimple.{OptionException, OptionParser, OptionSet}
import kafka.api.OffsetRequest
import kafka.common.{MessageFormatter, StreamEndException}
import kafka.consumer._
import kafka.message.{DefaultCompressionCodec, NoCompressionCodec}
import kafka.metrics.KafkaMetricsReporter
import kafka.producer.NewShinyProducer
import kafka.serializer.DefaultEncoder
import kafka.tools.ConsoleProducer.LineMessageReader
import kafka.tools.DefaultMessageFormatter
import kafka.utils._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._

object KafkaToKafka extends Logging {

  def main(args: Array[String]): Unit = {
    var msgQ = new LinkedBlockingQueue[BaseConsumerRecord]
    var recCurCount = 0
    var recLastCount = 0
    var proCurCount = 0
    var proLastCount = 0
    var times = 1
    val conf = new ConsumerConfig(args)
    val config = new ProducerConfig(args)
    val props = getOldConsumerProps(conf)
    val consumer = new OldConsumer(conf.filterSpec, props)
    val producer = new NewShinyProducer(getNewProducerProps(config))
    val prob = config.prob;
    println(s"Probability: $prob")
    Thread.sleep(1000)
    val rand = new Random()
    new Thread(new Runnable {
      override def run() = {
        while (true) {
          Thread.sleep(1000)
          val recDiff = recCurCount - recLastCount
          val proDiff = proCurCount - proLastCount
          recLastCount = recCurCount
          proLastCount = proCurCount
          val recAverage = recCurCount / times
          val proAverage = proCurCount / times
          times += 1
          println(s"Input  Throughput: $recDiff (per/sec)")
          println(s"Output Throughput: $proDiff (per/sec)")
          if (times % 10 == 0) {
            println(s"recAverage: $recAverage (per/sec)")
            println(s"proAverage: $proAverage (per/sec)")
          }
        }
      }
    }).start()
    new Thread(new Runnable {
      override def run() = {
        while (true) {
          val msg: BaseConsumerRecord = try {
            consumer.receive()
          } catch {
            case _: StreamEndException =>
              trace("Caught StreamEndException because consumer is shutdown, ignore and terminate.")
              // Consumer is already closed
              Exit.exit(1)
            case _: WakeupException =>
              trace("Caught WakeupException because consumer is shutdown, ignore and terminate.")
              // Consumer will be closed
              Exit.exit(1)
            case e: Throwable =>
              error("Error processing message, terminating consumer process: ", e)
              // Consumer will be closed
              Exit.exit(1)
          }
          recCurCount += 1
          if (rand.nextDouble() <= prob) {
            msgQ.put(msg)
          }
        }
      }
    }).start()
    while (true) {
      val msg: BaseConsumerRecord = msgQ.take()
      producer.send(config.topic, msg.key, msg.value)
      proCurCount += 1
    }
  }

  /**
    * Used by both getNewConsumerProps and getOldConsumerProps to retrieve the correct value for the
    * consumer parameter 'auto.offset.reset'.
    * Order of priority is:
    *   1. Explicitly set parameter via --consumer.property command line parameter
    *   2. Explicit --from-beginning given -> 'earliest'
    *   3. Default value of 'latest'
    *
    * In case both --from-beginning and an explicit value are specified an error is thrown if these
    * are conflicting.
    */
  def setAutoOffsetResetValue(config: ConsumerConfig, props: Properties) {
    val (earliestConfigValue, latestConfigValue) = if (config.useOldConsumer)
      (OffsetRequest.SmallestTimeString, OffsetRequest.LargestTimeString)
    else
      ("earliest", "latest")

    if (props.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
      // auto.offset.reset parameter was specified on the command line
      val autoResetOption = props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
      if (config.options.has(config.resetBeginningOpt) && earliestConfigValue != autoResetOption) {
        // conflicting options - latest und earliest, throw an error
        System.err.println(s"Can't simultaneously specify --from-beginning and 'auto.offset.reset=$autoResetOption', " +
          "please remove one option")
        Exit.exit(1)
      }
      // nothing to do, checking for valid parameter values happens later and the specified
      // value was already copied during .putall operation
    } else {
      // no explicit value for auto.offset.reset was specified
      // if --from-beginning was specified use earliest, otherwise default to latest
      val autoResetOption = if (config.options.has(config.resetBeginningOpt)) earliestConfigValue else latestConfigValue
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoResetOption)
    }
  }

  class ConsumerConfig(args: Array[String]) {
    val parser = new OptionParser(false)
    val topicIdOpt = parser.accepts("consumer-topic", "The topic id to consume on.")
      .withRequiredArg
      .describedAs("consumer-topic")
      .ofType(classOf[String])
    val whitelistOpt = parser.accepts("whitelist", "Whitelist of topics to include for consumption.")
      .withRequiredArg
      .describedAs("whitelist")
      .ofType(classOf[String])
    val blacklistOpt = parser.accepts("blacklist", "Blacklist of topics to exclude from consumption.")
      .withRequiredArg
      .describedAs("blacklist")
      .ofType(classOf[String])
    val partitionIdOpt = parser.accepts("partition", "The partition to consume from. Consumption " +
      "starts from the end of the partition unless '--offset' is specified.")
      .withRequiredArg
      .describedAs("partition")
      .ofType(classOf[java.lang.Integer])
    val offsetOpt = parser.accepts("offset", "The offset id to consume from (a non-negative number), or 'earliest' which means from beginning, or 'latest' which means from end")
      .withRequiredArg
      .describedAs("consume offset")
      .ofType(classOf[String])
      .defaultsTo("latest")
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED (only when using old consumer): The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])
    val consumerPropertyOpt = parser.accepts("consumer-property", "A mechanism to pass user-defined properties in the form key=value to the consumer.")
      .withRequiredArg
      .describedAs("consumer_prop")
      .ofType(classOf[String])
    val consumerConfigOpt = parser.accepts("consumer.config", s"Consumer config properties file. Note that ${consumerPropertyOpt} takes precedence over this config.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val messageFormatterOpt = parser.accepts("formatter", "The name of a class to use for formatting kafka messages for display.")
      .withRequiredArg
      .describedAs("class")
      .ofType(classOf[String])
      .defaultsTo(classOf[DefaultMessageFormatter].getName)
    val messageFormatterArgOpt = parser.accepts("property", "The properties to initialize the message formatter.")
      .withRequiredArg
      .describedAs("prop")
      .ofType(classOf[String])
    val deleteConsumerOffsetsOpt = parser.accepts("delete-consumer-offsets", "If specified, the consumer path in zookeeper is deleted when starting up")
    val resetBeginningOpt = parser.accepts("from-beginning", "If the consumer does not already have an established offset to consume from, " +
      "start with the earliest message present in the log rather than the latest message.")
    val maxMessagesOpt = parser.accepts("max-messages", "The maximum number of messages to consume before exiting. If not set, consumption is continual.")
      .withRequiredArg
      .describedAs("num_messages")
      .ofType(classOf[java.lang.Integer])
    val timeoutMsOpt = parser.accepts("timeout-ms", "If specified, exit if no message is available for consumption for the specified interval.")
      .withRequiredArg
      .describedAs("timeout_ms")
      .ofType(classOf[java.lang.Integer])
    val skipMessageOnErrorOpt = parser.accepts("skip-message-on-error", "If there is an error when processing a message, " +
      "skip it instead of halt.")
    val csvMetricsReporterEnabledOpt = parser.accepts("csv-reporter-enabled", "If set, the CSV metrics reporter will be enabled")
    val metricsDirectoryOpt = parser.accepts("metrics-dir", "If csv-reporter-enable is set, and this parameter is" +
      "set, the csv metrics will be output here")
      .withRequiredArg
      .describedAs("metrics directory")
      .ofType(classOf[java.lang.String])
    val newConsumerOpt = parser.accepts("new-consumer", "Use the new consumer implementation. This is the default, so " +
      "this option is deprecated and will be removed in a future release.")
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED (unless old consumer is used): The server to connect to.")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])
    val keyDeserializerOpt = parser.accepts("key-deserializer")
      .withRequiredArg
      .describedAs("deserializer for key")
      .ofType(classOf[String])
    val valueDeserializerOpt = parser.accepts("value-deserializer")
      .withRequiredArg
      .describedAs("deserializer for values")
      .ofType(classOf[String])
    val enableSystestEventsLoggingOpt = parser.accepts("enable-systest-events",
      "Log lifecycle events of the consumer in addition to logging consumed " +
        "messages. (This is specific for system tests.)")
    val isolationLevelOpt = parser.accepts("isolation-level",
      "Set to read_committed in order to filter out transactional messages which are not committed. Set to read_uncommitted" +
        "to read all messages.")
      .withRequiredArg()
      .ofType(classOf[String])
      .defaultsTo("read_uncommitted")
    val probOpt = parser.accepts("prob", "The probability of sending the record to producer")
      .withRequiredArg()
      .describedAs("probability")
      .ofType(classOf[Double])
    // placeholder
    val topicOpt = parser.accepts("producer-topic", "REQUIRED: The topic id to produce messages to.")
      .withRequiredArg
      .describedAs("producer-topic")
      .ofType(classOf[String])
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
      .withRequiredArg
      .describedAs("broker-list")
      .ofType(classOf[String])
    val syncOpt = parser.accepts("sync", "If set message send requests to the brokers are synchronously, one at a time as they arrive.")
    val compressionCodecOpt = parser.accepts("compression-codec", "The compression codec: either 'none', 'gzip', 'snappy', or 'lz4'." +
      "If specified without value, then it defaults to 'gzip'")
      .withOptionalArg()
      .describedAs("compression-codec")
      .ofType(classOf[String])
    val batchSizeOpt = parser.accepts("batch-size", "Number of messages to send in a single batch if they are not being sent synchronously.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(200)
    val messageSendMaxRetriesOpt = parser.accepts("message-send-max-retries", "Brokers can fail receiving the message for multiple reasons, and being unavailable transiently is just one of them. This property specifies the number of retires before the producer give up and drop this message.")
      .withRequiredArg
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(3)
    val retryBackoffMsOpt = parser.accepts("retry-backoff-ms", "Before each retry, the producer refreshes the metadata of relevant topics. Since leader election takes a bit of time, this property specifies the amount of time that the producer waits before refreshing the metadata.")
      .withRequiredArg
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(100)
    val sendTimeoutOpt = parser.accepts("timeout", "If set and the producer is running in asynchronous mode, this gives the maximum amount of time" +
      " a message will queue awaiting sufficient batch size. The value is given in ms.")
      .withRequiredArg
      .describedAs("timeout_ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1000)
    val queueSizeOpt = parser.accepts("queue-size", "If set and the producer is running in asynchronous mode, this gives the maximum amount of " +
      " messages will queue awaiting sufficient batch size.")
      .withRequiredArg
      .describedAs("queue_size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(10000)
    val queueEnqueueTimeoutMsOpt = parser.accepts("queue-enqueuetimeout-ms", "Timeout for event enqueue")
      .withRequiredArg
      .describedAs("queue enqueuetimeout ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(Int.MaxValue)
    val requestRequiredAcksOpt = parser.accepts("request-required-acks", "The required acks of the producer requests")
      .withRequiredArg
      .describedAs("request required acks")
      .ofType(classOf[java.lang.String])
      .defaultsTo("1")
    val requestTimeoutMsOpt = parser.accepts("request-timeout-ms", "The ack timeout of the producer requests. Value must be non-negative and non-zero")
      .withRequiredArg
      .describedAs("request timeout ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1500)
    val metadataExpiryMsOpt = parser.accepts("metadata-expiry-ms",
      "The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any leadership changes.")
      .withRequiredArg
      .describedAs("metadata expiration interval")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(5 * 60 * 1000L)
    val maxBlockMsOpt = parser.accepts("max-block-ms",
      "The max time that the producer will block for during a send request")
      .withRequiredArg
      .describedAs("max block on send")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(60 * 1000L)
    val maxMemoryBytesOpt = parser.accepts("max-memory-bytes",
      "The total memory used by the producer to buffer records waiting to be sent to the server.")
      .withRequiredArg
      .describedAs("total memory in bytes")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(32 * 1024 * 1024L)
    val maxPartitionMemoryBytesOpt = parser.accepts("max-partition-memory-bytes",
      "The buffer size allocated for a partition. When records are received which are smaller than this size the producer " +
        "will attempt to optimistically group them together until this size is reached.")
      .withRequiredArg
      .describedAs("memory in bytes per partition")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(16 * 1024L)
    val valueEncoderOpt = parser.accepts("value-serializer", "The class name of the message encoder implementation to use for serializing values.")
      .withRequiredArg
      .describedAs("encoder_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[DefaultEncoder].getName)
    val keyEncoderOpt = parser.accepts("key-serializer", "The class name of the message encoder implementation to use for serializing keys.")
      .withRequiredArg
      .describedAs("encoder_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[DefaultEncoder].getName)
    val messageReaderOpt = parser.accepts("line-reader", "The class name of the class to use for reading lines from standard in. " +
      "By default each line is read as a separate message.")
      .withRequiredArg
      .describedAs("reader_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[LineMessageReader].getName)
    val socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1024 * 100)
    val propertyOpt = parser.accepts("property", "A mechanism to pass user-defined properties in the form key=value to the message reader. " +
      "This allows custom configuration for a user-defined message reader.")
      .withRequiredArg
      .describedAs("prop")
      .ofType(classOf[String])
    val producerPropertyOpt = parser.accepts("producer-property", "A mechanism to pass user-defined properties in the form key=value to the producer. ")
      .withRequiredArg
      .describedAs("producer_prop")
      .ofType(classOf[String])
    val producerConfigOpt = parser.accepts("producer.config", s"Producer config properties file. Note that $producerPropertyOpt takes precedence over this config.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val useOldProducerOpt = parser.accepts("old-producer", "Use the old producer implementation.")


    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "The console consumer is a tool that reads data from Kafka and outputs it to standard output.")

    var groupIdPassed = true
    val options: OptionSet = tryParse(parser, args)
    val useOldConsumer = options.has(zkConnectOpt)
    val enableSystestEventsLogging = options.has(enableSystestEventsLoggingOpt)

    // If using old consumer, exactly one of whitelist/blacklist/topic is required.
    // If using new consumer, topic must be specified.
    var topicArg: String = null
    var whitelistArg: String = null
    var filterSpec: TopicFilter = null
    val extraConsumerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(consumerPropertyOpt).asScala)
    val consumerProps = if (options.has(consumerConfigOpt))
      Utils.loadProps(options.valueOf(consumerConfigOpt))
    else
      new Properties()
    val zkConnectionStr = options.valueOf(zkConnectOpt)
    val fromBeginning = options.has(resetBeginningOpt)
    val partitionArg = if (options.has(partitionIdOpt)) Some(options.valueOf(partitionIdOpt).intValue) else None
    val skipMessageOnError = options.has(skipMessageOnErrorOpt)
    val messageFormatterClass = Class.forName(options.valueOf(messageFormatterOpt))
    val formatterArgs = CommandLineUtils.parseKeyValueArgs(options.valuesOf(messageFormatterArgOpt).asScala)
    val maxMessages = if (options.has(maxMessagesOpt)) options.valueOf(maxMessagesOpt).intValue else -1
    val timeoutMs = if (options.has(timeoutMsOpt)) options.valueOf(timeoutMsOpt).intValue else -1
    val bootstrapServer = options.valueOf(bootstrapServerOpt)
    val keyDeserializer = options.valueOf(keyDeserializerOpt)
    val valueDeserializer = options.valueOf(valueDeserializerOpt)
    val isolationLevel = options.valueOf(isolationLevelOpt).toString
    val formatter: MessageFormatter = messageFormatterClass.newInstance().asInstanceOf[MessageFormatter]

    if (keyDeserializer != null && !keyDeserializer.isEmpty) {
      formatterArgs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer)
    }
    if (valueDeserializer != null && !valueDeserializer.isEmpty) {
      formatterArgs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer)
    }
    formatter.init(formatterArgs)

    if (useOldConsumer) {
      if (options.has(bootstrapServerOpt))
        CommandLineUtils.printUsageAndDie(parser, s"Option $bootstrapServerOpt is not valid with $zkConnectOpt.")
      else if (options.has(newConsumerOpt))
        CommandLineUtils.printUsageAndDie(parser, s"Option $newConsumerOpt is not valid with $zkConnectOpt.")
      val topicOrFilterOpt = List(topicIdOpt, whitelistOpt, blacklistOpt).filter(options.has)
      if (topicOrFilterOpt.size != 1)
        CommandLineUtils.printUsageAndDie(parser, "Exactly one of whitelist/blacklist/topic is required.")
      topicArg = options.valueOf(topicOrFilterOpt.head)
      filterSpec = if (options.has(blacklistOpt)) new Blacklist(topicArg) else new Whitelist(topicArg)
      Console.err.println("Using the ConsoleConsumer with old consumer is deprecated and will be removed " +
        s"in a future major release. Consider using the new consumer by passing $bootstrapServerOpt instead of ${zkConnectOpt}.")
    } else {
      val topicOrFilterOpt = List(topicIdOpt, whitelistOpt).filter(options.has)
      if (topicOrFilterOpt.size != 1)
        CommandLineUtils.printUsageAndDie(parser, "Exactly one of whitelist/topic is required.")
      topicArg = options.valueOf(topicIdOpt)
      whitelistArg = options.valueOf(whitelistOpt)
    }

    if (useOldConsumer && (partitionArg.isDefined || options.has(offsetOpt)))
      CommandLineUtils.printUsageAndDie(parser, "Partition-offset based consumption is supported in the new consumer only.")

    if (partitionArg.isDefined) {
      if (!options.has(topicIdOpt))
        CommandLineUtils.printUsageAndDie(parser, "The topic is required when partition is specified.")
      if (fromBeginning && options.has(offsetOpt))
        CommandLineUtils.printUsageAndDie(parser, "Options from-beginning and offset cannot be specified together.")
    } else if (options.has(offsetOpt))
      CommandLineUtils.printUsageAndDie(parser, "The partition is required when offset is specified.")

    def invalidOffset(offset: String): Nothing =
      CommandLineUtils.printUsageAndDie(parser, s"The provided offset value '$offset' is incorrect. Valid values are " +
        "'earliest', 'latest', or a non-negative long.")

    val offsetArg =
      if (options.has(offsetOpt)) {
        options.valueOf(offsetOpt).toLowerCase(Locale.ROOT) match {
          case "earliest" => OffsetRequest.EarliestTime
          case "latest" => OffsetRequest.LatestTime
          case offsetString =>
            val offset =
              try offsetString.toLong
              catch {
                case _: NumberFormatException => invalidOffset(offsetString)
              }
            if (offset < 0) invalidOffset(offsetString)
            offset
        }
      }
      else if (fromBeginning) OffsetRequest.EarliestTime
      else OffsetRequest.LatestTime

    if (!useOldConsumer) {
      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)

      if (options.has(newConsumerOpt)) {
        Console.err.println("The --new-consumer option is deprecated and will be removed in a future major release." +
          "The new consumer is used by default if the --bootstrap-server option is provided.")
      }
    }

    if (options.has(csvMetricsReporterEnabledOpt)) {
      val csvReporterProps = new Properties()
      csvReporterProps.put("kafka.metrics.polling.interval.secs", "5")
      csvReporterProps.put("kafka.metrics.reporters", "kafka.metrics.KafkaCSVMetricsReporter")
      if (options.has(metricsDirectoryOpt))
        csvReporterProps.put("kafka.csv.metrics.dir", options.valueOf(metricsDirectoryOpt))
      else
        csvReporterProps.put("kafka.csv.metrics.dir", "kafka_metrics")
      csvReporterProps.put("kafka.csv.metrics.reporter.enabled", "true")
      val verifiableProps = new VerifiableProperties(csvReporterProps)
      KafkaMetricsReporter.startReporters(verifiableProps)
    }

    //Provide the consumer with a randomly assigned group id
    if (!consumerProps.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
      consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, s"console-consumer-${new Random().nextInt(100000)}")
      groupIdPassed = false
    }

    def tryParse(parser: OptionParser, args: Array[String]): OptionSet = {
      try
        parser.parse(args: _*)
      catch {
        case e: OptionException =>
          CommandLineUtils.printUsageAndDie(parser, e.getMessage)
      }
    }
  }

  def getOldConsumerProps(config: ConsumerConfig): Properties = {
    val props = new Properties

    props.putAll(config.consumerProps)
    props.putAll(config.extraConsumerProps)
    setAutoOffsetResetValue(config, props)
    props.put("zookeeper.connect", config.zkConnectionStr)

    if (config.timeoutMs >= 0)
      props.put("consumer.timeout.ms", config.timeoutMs.toString)

    props
  }

  class ProducerConfig(args: Array[String]) {
    val parser = new OptionParser(false)
    val topicOpt = parser.accepts("producer-topic", "REQUIRED: The topic id to produce messages to.")
      .withRequiredArg
      .describedAs("producer-topic")
      .ofType(classOf[String])
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
      .withRequiredArg
      .describedAs("broker-list")
      .ofType(classOf[String])
    val syncOpt = parser.accepts("sync", "If set message send requests to the brokers are synchronously, one at a time as they arrive.")
    val compressionCodecOpt = parser.accepts("compression-codec", "The compression codec: either 'none', 'gzip', 'snappy', or 'lz4'." +
      "If specified without value, then it defaults to 'gzip'")
      .withOptionalArg()
      .describedAs("compression-codec")
      .ofType(classOf[String])
    val batchSizeOpt = parser.accepts("batch-size", "Number of messages to send in a single batch if they are not being sent synchronously.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(200)
    val messageSendMaxRetriesOpt = parser.accepts("message-send-max-retries", "Brokers can fail receiving the message for multiple reasons, and being unavailable transiently is just one of them. This property specifies the number of retires before the producer give up and drop this message.")
      .withRequiredArg
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(3)
    val retryBackoffMsOpt = parser.accepts("retry-backoff-ms", "Before each retry, the producer refreshes the metadata of relevant topics. Since leader election takes a bit of time, this property specifies the amount of time that the producer waits before refreshing the metadata.")
      .withRequiredArg
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(100)
    val sendTimeoutOpt = parser.accepts("timeout", "If set and the producer is running in asynchronous mode, this gives the maximum amount of time" +
      " a message will queue awaiting sufficient batch size. The value is given in ms.")
      .withRequiredArg
      .describedAs("timeout_ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1000)
    val queueSizeOpt = parser.accepts("queue-size", "If set and the producer is running in asynchronous mode, this gives the maximum amount of " +
      " messages will queue awaiting sufficient batch size.")
      .withRequiredArg
      .describedAs("queue_size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(10000)
    val queueEnqueueTimeoutMsOpt = parser.accepts("queue-enqueuetimeout-ms", "Timeout for event enqueue")
      .withRequiredArg
      .describedAs("queue enqueuetimeout ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(Int.MaxValue)
    val requestRequiredAcksOpt = parser.accepts("request-required-acks", "The required acks of the producer requests")
      .withRequiredArg
      .describedAs("request required acks")
      .ofType(classOf[java.lang.String])
      .defaultsTo("1")
    val requestTimeoutMsOpt = parser.accepts("request-timeout-ms", "The ack timeout of the producer requests. Value must be non-negative and non-zero")
      .withRequiredArg
      .describedAs("request timeout ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1500)
    val metadataExpiryMsOpt = parser.accepts("metadata-expiry-ms",
      "The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any leadership changes.")
      .withRequiredArg
      .describedAs("metadata expiration interval")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(5 * 60 * 1000L)
    val maxBlockMsOpt = parser.accepts("max-block-ms",
      "The max time that the producer will block for during a send request")
      .withRequiredArg
      .describedAs("max block on send")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(60 * 1000L)
    val maxMemoryBytesOpt = parser.accepts("max-memory-bytes",
      "The total memory used by the producer to buffer records waiting to be sent to the server.")
      .withRequiredArg
      .describedAs("total memory in bytes")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(32 * 1024 * 1024L)
    val maxPartitionMemoryBytesOpt = parser.accepts("max-partition-memory-bytes",
      "The buffer size allocated for a partition. When records are received which are smaller than this size the producer " +
        "will attempt to optimistically group them together until this size is reached.")
      .withRequiredArg
      .describedAs("memory in bytes per partition")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(16 * 1024L)
    val valueEncoderOpt = parser.accepts("value-serializer", "The class name of the message encoder implementation to use for serializing values.")
      .withRequiredArg
      .describedAs("encoder_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[DefaultEncoder].getName)
    val keyEncoderOpt = parser.accepts("key-serializer", "The class name of the message encoder implementation to use for serializing keys.")
      .withRequiredArg
      .describedAs("encoder_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[DefaultEncoder].getName)
    val messageReaderOpt = parser.accepts("line-reader", "The class name of the class to use for reading lines from standard in. " +
      "By default each line is read as a separate message.")
      .withRequiredArg
      .describedAs("reader_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[LineMessageReader].getName)
    val socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1024 * 100)
    val propertyOpt = parser.accepts("property", "A mechanism to pass user-defined properties in the form key=value to the message reader. " +
      "This allows custom configuration for a user-defined message reader.")
      .withRequiredArg
      .describedAs("prop")
      .ofType(classOf[String])
    val producerPropertyOpt = parser.accepts("producer-property", "A mechanism to pass user-defined properties in the form key=value to the producer. ")
      .withRequiredArg
      .describedAs("producer_prop")
      .ofType(classOf[String])
    val producerConfigOpt = parser.accepts("producer.config", s"Producer config properties file. Note that $producerPropertyOpt takes precedence over this config.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val useOldProducerOpt = parser.accepts("old-producer", "Use the old producer implementation.")
    val probOpt = parser.accepts("prob", "The probability of sending the record to producer")
      .withRequiredArg()
      .describedAs("probability")
      .ofType(classOf[Double])
    // placeholder
    val topicIdOpt = parser.accepts("consumer-topic", "The topic id to consume on.")
      .withRequiredArg
      .describedAs("consumer-topic")
      .ofType(classOf[String])
    val whitelistOpt = parser.accepts("whitelist", "Whitelist of topics to include for consumption.")
      .withRequiredArg
      .describedAs("whitelist")
      .ofType(classOf[String])
    val blacklistOpt = parser.accepts("blacklist", "Blacklist of topics to exclude from consumption.")
      .withRequiredArg
      .describedAs("blacklist")
      .ofType(classOf[String])
    val partitionIdOpt = parser.accepts("partition", "The partition to consume from. Consumption " +
      "starts from the end of the partition unless '--offset' is specified.")
      .withRequiredArg
      .describedAs("partition")
      .ofType(classOf[java.lang.Integer])
    val offsetOpt = parser.accepts("offset", "The offset id to consume from (a non-negative number), or 'earliest' which means from beginning, or 'latest' which means from end")
      .withRequiredArg
      .describedAs("consume offset")
      .ofType(classOf[String])
      .defaultsTo("latest")
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED (only when using old consumer): The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])
    val consumerPropertyOpt = parser.accepts("consumer-property", "A mechanism to pass user-defined properties in the form key=value to the consumer.")
      .withRequiredArg
      .describedAs("consumer_prop")
      .ofType(classOf[String])
    val consumerConfigOpt = parser.accepts("consumer.config", s"Consumer config properties file. Note that ${consumerPropertyOpt} takes precedence over this config.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val messageFormatterOpt = parser.accepts("formatter", "The name of a class to use for formatting kafka messages for display.")
      .withRequiredArg
      .describedAs("class")
      .ofType(classOf[String])
      .defaultsTo(classOf[DefaultMessageFormatter].getName)
    val messageFormatterArgOpt = parser.accepts("property", "The properties to initialize the message formatter.")
      .withRequiredArg
      .describedAs("prop")
      .ofType(classOf[String])
    val deleteConsumerOffsetsOpt = parser.accepts("delete-consumer-offsets", "If specified, the consumer path in zookeeper is deleted when starting up")
    val resetBeginningOpt = parser.accepts("from-beginning", "If the consumer does not already have an established offset to consume from, " +
      "start with the earliest message present in the log rather than the latest message.")
    val maxMessagesOpt = parser.accepts("max-messages", "The maximum number of messages to consume before exiting. If not set, consumption is continual.")
      .withRequiredArg
      .describedAs("num_messages")
      .ofType(classOf[java.lang.Integer])
    val timeoutMsOpt = parser.accepts("timeout-ms", "If specified, exit if no message is available for consumption for the specified interval.")
      .withRequiredArg
      .describedAs("timeout_ms")
      .ofType(classOf[java.lang.Integer])
    val skipMessageOnErrorOpt = parser.accepts("skip-message-on-error", "If there is an error when processing a message, " +
      "skip it instead of halt.")
    val csvMetricsReporterEnabledOpt = parser.accepts("csv-reporter-enabled", "If set, the CSV metrics reporter will be enabled")
    val metricsDirectoryOpt = parser.accepts("metrics-dir", "If csv-reporter-enable is set, and this parameter is" +
      "set, the csv metrics will be output here")
      .withRequiredArg
      .describedAs("metrics directory")
      .ofType(classOf[java.lang.String])
    val newConsumerOpt = parser.accepts("new-consumer", "Use the new consumer implementation. This is the default, so " +
      "this option is deprecated and will be removed in a future release.")
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED (unless old consumer is used): The server to connect to.")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])
    val keyDeserializerOpt = parser.accepts("key-deserializer")
      .withRequiredArg
      .describedAs("deserializer for key")
      .ofType(classOf[String])
    val valueDeserializerOpt = parser.accepts("value-deserializer")
      .withRequiredArg
      .describedAs("deserializer for values")
      .ofType(classOf[String])
    val enableSystestEventsLoggingOpt = parser.accepts("enable-systest-events",
      "Log lifecycle events of the consumer in addition to logging consumed " +
        "messages. (This is specific for system tests.)")
    val isolationLevelOpt = parser.accepts("isolation-level",
      "Set to read_committed in order to filter out transactional messages which are not committed. Set to read_uncommitted" +
        "to read all messages.")
      .withRequiredArg()
      .ofType(classOf[String])
      .defaultsTo("read_uncommitted")

    val options = parser.parse(args: _*)
    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Read data from standard input and publish it to Kafka.")
    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, brokerListOpt)

    val useOldProducer = options.has(useOldProducerOpt)
    val topic = options.valueOf(topicOpt)
    val brokerList = options.valueOf(brokerListOpt)
    ToolsUtils.validatePortOrDie(parser, brokerList)
    val sync = options.has(syncOpt)
    val compressionCodecOptionValue = options.valueOf(compressionCodecOpt)
    val compressionCodec = if (options.has(compressionCodecOpt))
      if (compressionCodecOptionValue == null || compressionCodecOptionValue.isEmpty)
        DefaultCompressionCodec.name
      else compressionCodecOptionValue
    else NoCompressionCodec.name
    val batchSize = options.valueOf(batchSizeOpt)
    val sendTimeout = options.valueOf(sendTimeoutOpt)
    val queueSize = options.valueOf(queueSizeOpt)
    val queueEnqueueTimeoutMs = options.valueOf(queueEnqueueTimeoutMsOpt)
    val requestRequiredAcks = options.valueOf(requestRequiredAcksOpt)
    val requestTimeoutMs = options.valueOf(requestTimeoutMsOpt)
    val messageSendMaxRetries = options.valueOf(messageSendMaxRetriesOpt)
    val retryBackoffMs = options.valueOf(retryBackoffMsOpt)
    val keyEncoderClass = options.valueOf(keyEncoderOpt)
    val valueEncoderClass = options.valueOf(valueEncoderOpt)
    val readerClass = options.valueOf(messageReaderOpt)
    val socketBuffer = options.valueOf(socketBufferSizeOpt)
    val cmdLineProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(propertyOpt).asScala)
    val extraProducerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(producerPropertyOpt).asScala)
    /* new producer related configs */
    val maxMemoryBytes = options.valueOf(maxMemoryBytesOpt)
    val maxPartitionMemoryBytes = options.valueOf(maxPartitionMemoryBytesOpt)
    val metadataExpiryMs = options.valueOf(metadataExpiryMsOpt)
    val maxBlockMs = options.valueOf(maxBlockMsOpt)
    val prob = if (options.has(probOpt))
      options.valueOf(probOpt)
    else
      1.0
  }


  private def producerProps(config: ProducerConfig): Properties = {
    val props =
      if (config.options.has(config.producerConfigOpt))
        Utils.loadProps(config.options.valueOf(config.producerConfigOpt))
      else new Properties
    props.putAll(config.extraProducerProps)
    props
  }

  def getNewProducerProps(config: ProducerConfig): Properties = {
    val props = producerProps(config)

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokerList)
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.compressionCodec)
    props.put(ProducerConfig.SEND_BUFFER_CONFIG, config.socketBuffer.toString)
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, config.retryBackoffMs.toString)
    props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, config.metadataExpiryMs.toString)
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, config.maxBlockMs.toString)
    props.put(ProducerConfig.ACKS_CONFIG, config.requestRequiredAcks)
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, config.requestTimeoutMs.toString)
    props.put(ProducerConfig.RETRIES_CONFIG, config.messageSendMaxRetries.toString)
    props.put(ProducerConfig.LINGER_MS_CONFIG, config.sendTimeout.toString)
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.maxMemoryBytes.toString)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.maxPartitionMemoryBytes.toString)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "console-producer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

    props
  }

}
