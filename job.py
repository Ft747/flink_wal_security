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


def generate_ordered_combinations(length=3):
    """Generate ordered letter combinations (aaa, aab, aac, ..., zzz)."""
    import itertools
    letters = string.ascii_lowercase
    combinations = []

    # Generate all possible combinations of specified length
    for combo in itertools.product(letters, repeat=length):
        combinations.append(''.join(combo))

    return combinations


def get_combination_stats(length):
    """Get statistics about combinations for a given length."""
    total_combinations = 26 ** length
    words_per_combination = 20000 / total_combinations
    return total_combinations, words_per_combination


def generate_words(count, vocab=None, combination_length=3):
    """
    Generate a fixed number of words from a vocabulary.
    Uses ordered letter combinations by default.

    :param count: how many words to generate
    :param vocab: list of words to choose from
    :param combination_length: length of letter combinations (3 or 4)
    :return: list of generated words
    """
    if vocab is None:
        # Generate ordered combinations of specified length
        vocab = generate_ordered_combinations(combination_length)

    if not vocab:
        raise ValueError("No vocabulary provided")

    return [random.choice(vocab) for _ in range(count)]


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(1000, CheckpointingMode.AT_LEAST_ONCE)

    # Configuration: choose 3 or 4 letter combinations
    combination_length = 3  # Change to 4 for 4-letter combinations

    # Generate ordered letter combinations - this will create a predictable distribution
    # with systematic combinations that are easier to track and debug
    combinations = generate_ordered_combinations(combination_length)
    total_combinations, avg_per_combo = get_combination_stats(combination_length)

    print(f"Using {len(combinations)} ordered {combination_length}-letter combinations")
    print(f"Expected ~{avg_per_combo:.1f} occurrences per combination")
    print(f"First 10: {combinations[:10]}")
    print(f"Last 10: {combinations[-10:]}")

    ds = env.from_collection(generate_words(20000, combination_length=combination_length), type_info=Types.STRING())
    keyed_state = ds.key_by(lambda x: x)
    res = keyed_state.process(
        WordCounter(), output_type=Types.TUPLE([Types.STRING(), Types.INT()])
    )
    res.print()
    env.execute("Word Counter")


if __name__ == "__main__":
    main()
