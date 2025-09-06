import logging
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    RuntimeContext,
    SourceFunction,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class CountPerKey(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        # ValueState descriptor for an int counter
        desc = ValueStateDescriptor("count_state", Types.INT())
        self.count_state = runtime_context.get_state(desc)

    def process_element(self, value, ctx):
        # value: the incoming number (we key by value % 10)
        current = self.count_state.value()
        if current is None:
            current = 0
        current += 1
        self.count_state.update(current)

        # Logging for visibility
        try:
            key = ctx.get_current_key()
        except Exception:
            key = value % 10
        print("processed value=%s key=%s count=%s", value, key, current)

        # emit (key, count) — printed by the print sink below
        yield (key, current)


def build_env():
    env = StreamExecutionEnvironment.get_execution_environment()

    # short checkpoint interval -> frequent durability (2s)
    env.enable_checkpointing(20, CheckpointingMode.EXACTLY_ONCE)

    # NOTE: configure a durable state backend (RocksDB or filesystem) in flink-conf.yaml
    # or via cluster config to ensure savepoints are persisted across restarts.

    return env


def main():
    env = build_env()

    # Create infinite source of numbers
    # ds = env.add_source(CounterSource()).uid("counter-source")
    ds = env.from_collection(range(10000)).uid("collection")

    # partition into 10 keys
    keyed = ds.key_by(lambda x: x)

    # stateful operator with stable uid
    result = keyed.process(CountPerKey()).uid("count-operator")

    # print sink + uid (debug; replace with real sink in production)
    result.print().uid("print-sink")

    env.execute("infinite-stateful-job")


if __name__ == "__main__":
    main()
