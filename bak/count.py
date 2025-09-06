# wal_force_persistent.py
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common.typeinfo import Types

# from pyflink.common.state import ValueStateDescriptor
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.common import CheckpointingMode
# from pyflink.datastream.checkpoint import CheckpointConfig


class BigStateCounter(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        # desc = ValueStateDescriptor("count", Types.LONG())
        # self.state = runtime_context.get_state(desc)
        pass

    def process_element(self, value, ctx):
        cnt = self.state.value()
        if cnt is None:
            cnt = 0
        # grow the state: increment; also write a blob-like string to simulate heavy bytes
        cnt += 1
        self.state.update(cnt)
        # return nothing important; side effect is state growth
        return


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(2)

    # 1) frequent checkpoints -> causes WAL activity
    env.enable_checkpointing(
        2000
    )  # 2s interval. triggers WAL writes. :contentReference[oaicite:11]{index=11}
    # env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

    # 2) retain checkpoints after cancellation (so files aren't auto-deleted)
    # env.get_checkpoint_config().enable_externalized_checkpoints(
    #     CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    # )  # keeps checkpoint files. :contentReference[oaicite:12]{index=12}

    # 3) force unaligned checkpoints so in-flight data is saved (more WAL churn)
    # env.get_checkpoint_config().enable_unaligned_checkpoints()  # PyFlink API. :contentReference[oaicite:13]{index=13}
    # force alignment-timeout -> 0 means start unaligned immediately
    # try:
    #     env.get_checkpoint_config().set_alignment_timeout(
    #         0
    #     )  # if API available; fallback to flink-conf.yaml. :contentReference[oaicite:14]{index=14}
    # except Exception:
    #     pass

    # 4) ensure checkpoint storage is filesystem (visible on disk)
    # env.get_checkpoint_config().set_checkpoint_storage(
    #     "file:///tmp/flink-checkpoints"
    # )  # visible WAL/ckp dirs. :contentReference[oaicite:15]{index=15}

    # 5) create a source that continuously produces many distinct keys to force large state
    #    generate an effectively infinite stream by cycling through a large keyspace
    KEYSPACE = 200000

    def gen():
        i = 0
        while True:
            yield str(i % KEYSPACE)
            i += 1

    ds = env.from_collection(gen(), type_info=Types.STRING())
    keyed = ds.key_by(lambda x: x)
    keyed.process(BigStateCounter())

    env.execute("wal-force-persist-job")


if __name__ == "__main__":
    main()
