from pyflink.common import Time, Types
from pyflink.datastream import StreamExecutionEnvironment, SourceFunction
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
import time


# --- Custom Source ---
class InfiniteNumbers(SourceFunction):
    def run(self, ctx):
        i = 0
        while True:
            # emit key "a" with increasing value
            ctx.collect(("a", float(i)))
            i += 1
            time.sleep(0.5)  # slow it down so you can observe

    def cancel(self):
        pass


# --- Stateful Process Function ---
class Sum(KeyedProcessFunction):
    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ValueStateDescriptor("state", Types.FLOAT())

        state_ttl_config = (
            StateTtlConfig.new_builder(Time.seconds(10))  # keep for 10s
            .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite)
            .disable_cleanup_in_background()
            .build()
        )
        state_descriptor.enable_time_to_live(state_ttl_config)
        self.state = runtime_context.get_state(state_descriptor)

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        current = self.state.value()
        if current is None:
            current = 0.0

        current += value[1]
        self.state.update(current)

        yield value[0], current


# --- Main Job ---
def state_access_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    ds = env.add_source(
        source_func=InfiniteNumbers(),
        type_info=Types.TUPLE([Types.STRING(), Types.FLOAT()]),
    )
    env.from_source
    ds.key_by(lambda v: v[0]).process(Sum()).print()

    env.execute("stateful-sum-demo")


if __name__ == "__main__":
    state_access_demo()
