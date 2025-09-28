import logging
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    RuntimeContext,
    SourceFunction,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types


class WordCounter(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        desc = ValueStateDescriptor("count", Types.INT())
        self.count_state = runtime_context.get_state(desc)

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        current = self.count_state.value()
        if current is None:
            current = 0
        current += 1
        self.count_state.update(current)
        yield current


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(500, CheckpointingMode.AT_LEAST_ONCE)
    ds = env.from_collection(list(range(20)), type_info=Types.INT())
    keyed_state = ds.key_by(lambda x: x)
    res = keyed_state.process(WordCounter(), output_type=Types.INT())
    res.print()
    env.execute("Word Counter")


if __name__ == "__main__":
    main()
