import random
import string
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
        yield (value, current)


def random_words():
    """Generate a random combination of five letters (uppercase + lowercase)."""
    return "".join(random.choice(string.ascii_letters) for _ in range(3))


def generate_words(count, vocab=None):
    """
    Generate a fixed number of words from a vocabulary.
    If no vocabulary is provided, generate random five-letter words.

    :param count: how many words to generate
    :param vocab: list of words to choose from
    :return: list of generated words
    """
    if vocab is None:
        # Instead of a fixed list, generate random five-letter words
        return [random_words() for _ in range(count)]

    return [random.choice(vocab) for _ in range(count)]


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(1000, CheckpointingMode.AT_LEAST_ONCE)
    ds = env.from_collection(generate_words(20000), type_info=Types.STRING())
    keyed_state = ds.key_by(lambda x: x)
    res = keyed_state.process(
        WordCounter(), output_type=Types.TUPLE([Types.STRING(), Types.INT()])
    )
    res.print()
    env.execute("Word Counter")


if __name__ == "__main__":
    main()
