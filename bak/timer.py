from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig


class TimerFunction(KeyedProcessFunction):
    def open(self, runtime_context):
        self.counter = runtime_context.get_state(
            ValueStateDescriptor("counter", Types.INT())
        )

    def process_element(self, value, ctx):
        current = self.counter.value() or 0
        self.counter.update(current + 1)  # Triggers WAL write
        ctx.timer_service().register_processing_time_timer(
            ctx.timer_service().current_processing_time() + 1000
        )
        yield f"Key: {value[0]}, Count: {self.counter.value()}"

    def on_timer(self, timestamp, ctx):
        yield f"Timer fired for key: {ctx.get_current_key()}"


env = StreamExecutionEnvironment.get_execution_environment()

ds = env.from_collection(
    [(f"user{i}", i) for i in range(1000)],
    type_info=Types.TUPLE([Types.STRING(), Types.INT()]),
)
ds.key_by(lambda x: x[0]).process(TimerFunction()).print()
env.execute()
