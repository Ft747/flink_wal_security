import random
from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig


class Sum(KeyedProcessFunction):
    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ValueStateDescriptor("state", Types.FLOAT())
        state_ttl_config = (
            StateTtlConfig.new_builder(Time.seconds(5))
            .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite)
            .disable_cleanup_in_background()
            .build()
        )
        state_descriptor.enable_time_to_live(state_ttl_config)
        self.state = runtime_context.get_state(state_descriptor)

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        # retrieve the current count
        current = self.state.value()
        if current is None:
            current = 0

        # update the state's count
        current += value[1]
        self.state.update(current)

        yield value[0], current


def state_access_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    names = ["Alice", "Bob", "Dave", "Anna"]
    data = []

    for i in range(10000):
        name = names[i % 2]  # Alternates between Alice and Bob
        value = round(
            random.uniform(1.0, 150.0), 1
        )  # Random float between 1.0 and 150.0
        data.append((name, value))
    ds = env.from_collection(
        collection=data,
        type_info=Types.TUPLE([Types.STRING(), Types.FLOAT()]),
    )

    # apply the process function onto a keyed stream
    ds.key_by(lambda value: value[0]).process(Sum()).print()

    # submit for execution
    env.execute()


if __name__ == "__main__":
    state_access_demo()
