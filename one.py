from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col
from pyflink.common import Configuration
from pyflink.common.typeinfo import Types


def main():
    # RocksDB via config (fits PyFlink 1.15–1.16)
    conf = Configuration()
    # conf.set_string("state.backend", "rocksdb")
    # conf.set_string("state.backend.rocksdb.localdir", "/tmp/rocksdb")
    # conf.set_string("state.checkpoints.dir", "file:///tmp/flink-checkpoints")
    # conf.set_boolean("state.backend.incremental", True)

    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.enable_checkpointing(5000)

    t_env = StreamTableEnvironment.create(env)

    # Ensure numeric typing (INT), not RAW
    numbers = range(1, 101)
    ds = env.from_collection(numbers, type_info=Types.INT())

    input_table = t_env.from_data_stream(ds)  # column "f0" is INT
    result_table = input_table.select((col("f0") * col("f0")).alias("square"))

    result_ds = t_env.to_data_stream(result_table)
    result_ds.print()

    env.execute("Square Numbers with RocksDB")


if __name__ == "__main__":
    main()
