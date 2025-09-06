# rolling_average_rocksdb.py
# Single-file PyFlink program that computes a rolling average for 10_000 numbers
# All lines are commented for clarity (per request).

# import StreamExecutionEnvironment for building the job
from pyflink.datastream import StreamExecutionEnvironment  # environment builder

# import function base classes and runtime context types
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    RuntimeContext,
)  # process function base

# import state descriptor helpers to declare ValueState
from pyflink.datastream.state import ValueStateDescriptor  # state descriptors

# import common type hints for ValueState types
from pyflink.common.typeinfo import Types  # Flink TypeInformation helpers

# import Embedded RocksDB backend class
from pyflink.datastream.state_backend import (
    EmbeddedRocksDBStateBackend,
)  # RocksDB-backed state backend
import time


# define a KeyedProcessFunction that maintains count, total, and average as ValueState
class RollingAverageFunction(KeyedProcessFunction):
    # initialize python-side attributes (state handles will be created in open)
    def __init__(self):
        super(RollingAverageFunction, self).__init__()  # call parent ctor
        self.count_state = None  # placeholder for ValueState[int]
        self.total_state = None  # placeholder for ValueState[float]
        self.avg_state = None  # placeholder for ValueState[float]

    # open is called once per parallel instance; initialize state descriptors here
    def open(self, runtime_context: RuntimeContext):
        # create a ValueStateDescriptor for count (long / integer)
        count_desc = ValueStateDescriptor(
            "count_state", Types.LONG()
        )  # state name "count_state"
        # obtain the state handle for count using the runtime context
        self.count_state = runtime_context.get_state(
            count_desc
        )  # ValueState for integer count

        # create a ValueStateDescriptor for total (double / float)
        total_desc = ValueStateDescriptor(
            "total_state", Types.DOUBLE()
        )  # state name "total_state"
        # obtain the state handle for total
        self.total_state = runtime_context.get_state(
            total_desc
        )  # ValueState for float total

        # create a ValueStateDescriptor for average (double / float) -- optional (redundant)
        avg_desc = ValueStateDescriptor(
            "avg_state", Types.DOUBLE()
        )  # state name "avg_state"
        # obtain the state handle for average
        self.avg_state = runtime_context.get_state(
            avg_desc
        )  # ValueState for float average

    # process_element is invoked for each incoming element in the keyed stream
    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        # retrieve the current key from the context (useful for multi-key jobs)
        current_key = ctx.get_current_key()  # key for which state is scoped

        # read current count from state; if None, treat as 0
        current_count = self.count_state.value()  # ValueState.value()
        if current_count is None:
            current_count = 0  # initialize count if absent

        # read current total from state; if None, treat as 0.0
        current_total = self.total_state.value()  # ValueState.value()
        if current_total is None:
            current_total = 0.0  # initialize total if absent

        # update count by one because an element arrived
        new_count = current_count + 1  # increment count by 1

        # update total by adding the numeric value (cast to float)
        new_total = float(current_total) + float(value)  # accumulate total as float

        # compute the new average (safe guard for division by zero)
        new_avg = new_total / new_count if new_count != 0 else 0.0  # compute average

        # persist updated count into count state
        self.count_state.update(new_count)  # write back count

        # persist updated total into total state
        self.total_state.update(new_total)  # write back total

        # persist computed average into avg state (optional; can be derived)
        self.avg_state.update(new_avg)  # write back average

        # emit a tuple (key, count, total, average) downstream for observation
        # using a simple Python tuple for demonstration
        # downstream can be .print() to see results
        # time.sleep(0.000005 * new_count)
        print(
            (
                f"current key {current_key}",
                f"new_count {new_count}",
                f"new_total {new_total}",
                f"new_avg {new_avg}",
            )
        )  # emit result


# main job builder
def build_and_run():
    # create the StreamExecutionEnvironment (job builder)
    env = (
        StreamExecutionEnvironment.get_execution_environment()
    )  # execution environment

    # set parallelism to 1 so single operator instance handles all keys (per request)
    env.set_parallelism(1)  # global parallelism = 1

    # enable checkpointing with a 5 ms interval (argument is milliseconds)
    env.enable_checkpointing(20)  # VERY FREQUENT: 5 ms (diagnostic/test use only)

    # create a bounded source of 10_000 numbers (0..9999)
    source = env.from_collection(list(range(25000)), type_info=Types.LONG()).uid(
        "numbers"
    )  # create collection source

    # key the stream by a constant key (0) so the operator is keyed and uses keyed state
    keyed = source.key_by(
        lambda x: x
    )  # all elements share the same key -> single-key aggregation

    # apply the RollingAverageFunction (KeyedProcessFunction) and print results
    processed = keyed.process(RollingAverageFunction()).uid(
        "roll_avg"
    )  # attach process function
    processed.print()  # print emitted tuples to stdout

    # execute the job (blocking call)
    env.execute("rolling-average-rocksdb-job")  # submit and run job


# guard so script can be imported without running
if __name__ == "__main__":
    build_and_run()  # run the job
