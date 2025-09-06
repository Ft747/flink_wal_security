from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.functions import KeyedCoProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
from pyflink.common import Configuration  # Import Configuration


class MultiplyFunction(KeyedCoProcessFunction):
    def __init__(self):
        self.state1 = None
        self.state2 = None

    def open(self, runtime_context: RuntimeContext):
        state_descriptor1 = ValueStateDescriptor("state1", Types.DOUBLE())
        state_descriptor2 = ValueStateDescriptor("state2", Types.DOUBLE())
        self.state1 = runtime_context.get_state(state_descriptor1)
        self.state2 = runtime_context.get_state(state_descriptor2)

    def process_element1(self, value, ctx: "KeyedCoProcessFunction.Context"):
        index, num1 = value
        num2 = self.state2.value()

        if num2 is not None:
            result = num1 * num2
            yield (index, result)
            self.state2.clear()
        else:
            self.state1.update(num1)

    def process_element2(self, value, ctx: "KeyedCoProcessFunction.Context"):
        index, num2 = value
        num1 = self.state1.value()

        if num1 is not None:
            result = num1 * num2
            yield (index, result)
            self.state1.clear()
        else:
            self.state2.update(num2)


def main():
    # Create configuration with state backend settings
    config = Configuration()
    config.set_string("state.backend", "filesystem")  # Use filesystem state backend
    config.set_string(
        "state.checkpoints.dir", "file:///tmp/flink-checkpoints"
    )  # WAL location

    # Create environment with configuration
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)

    # Configure checkpointing with WAL
    env.enable_checkpointing(5000)  # Checkpoint every 5 seconds

    # Configure checkpointing mode
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

    # Additional checkpoint configurations
    env.get_checkpoint_config().set_min_pause_between_checkpoints(
        1000
    )  # 1 second pause
    env.get_checkpoint_config().set_checkpoint_timeout(60000)  # 60 seconds timeout
    env.get_checkpoint_config().set_max_concurrent_checkpoints(
        1
    )  # One checkpoint at a time
    env.get_checkpoint_config().set_externalized_checkpoint_retention(
        "RETAIN_ON_CANCELLATION"
    )

    # Define two collections of 10 numbers each
    collection1 = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    collection2 = [10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]

    # Create data streams with index-value pairs
    ds1 = env.from_collection(
        [(i, x) for i, x in enumerate(collection1)],
        type_info=Types.TUPLE([Types.INT(), Types.DOUBLE()]),
    )
    ds2 = env.from_collection(
        [(i, x) for i, x in enumerate(collection2)],
        type_info=Types.TUPLE([Types.INT(), Types.DOUBLE()]),
    )

    # Connect the two streams and process
    connected_streams = ds1.connect(ds2)
    result_stream = connected_streams.key_by(lambda x: x[0], lambda x: x[0]).process(
        MultiplyFunction()
    )

    # Print the results
    result_stream.print()

    # Execute the Flink job
    env.execute("Multiply Two Collections with WAL")


if __name__ == "__main__":
    main()
