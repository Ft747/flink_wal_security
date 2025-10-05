import random
import string
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types


def random_words():
    """Generate a random combination of three letters."""
    return "".join(random.choice(string.ascii_letters) for _ in range(3))


def generate_words(count, vocab=None):
    """Generate a fixed number of words from a vocabulary."""
    if vocab is None:
        return [random_words() for _ in range(count)]
    return [random.choice(vocab) for _ in range(count)]


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Simple word generation and counting without state
    words = generate_words(100)  # Reduced to 100 words for faster execution
    ds = env.from_collection(words, type_info=Types.STRING())
    
    # Simple map operation to create word count pairs
    word_counts = ds.map(lambda word: (word, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
    
    # Group by word and sum counts
    result = word_counts.key_by(lambda x: x[0]).reduce(
        lambda a, b: (a[0], a[1] + b[1])
    )
    
    result.print()
    env.execute("Simple Word Counter")


if __name__ == "__main__":
    main()
