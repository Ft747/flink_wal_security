################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import random
import math
from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import (
    ListStateDescriptor,
    ValueStateDescriptor,
    StateTtlConfig,
)
import numpy as np


class FeatureEngineeringProcessor(KeyedProcessFunction):
    def __init__(self, window_size=10, percentiles=[0.25, 0.5, 0.75]):
        """
        Real-time feature engineering processor.

        :param window_size: Size of rolling window for calculations
        :param percentiles: Percentiles to calculate
        """
        self.window_size = window_size
        self.percentiles = percentiles

        # State variables
        self.values_state = None
        self.min_state = None
        self.max_state = None
        self.sum_state = None
        self.sum_squares_state = None
        self.count_state = None

    def open(self, runtime_context: RuntimeContext):
        """Initialize all state descriptors."""
        # TTL configuration
        state_ttl_config = (
            StateTtlConfig.new_builder(Time.hours(2))
            .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite)
            .build()
        )

        # Rolling window of values
        values_descriptor = ListStateDescriptor("values", Types.FLOAT())
        values_descriptor.enable_time_to_live(state_ttl_config)
        self.values_state = runtime_context.get_list_state(values_descriptor)

        # Min value state
        min_descriptor = ValueStateDescriptor("min_value", Types.FLOAT())
        min_descriptor.enable_time_to_live(state_ttl_config)
        self.min_state = runtime_context.get_state(min_descriptor)

        # Max value state
        max_descriptor = ValueStateDescriptor("max_value", Types.FLOAT())
        max_descriptor.enable_time_to_live(state_ttl_config)
        self.max_state = runtime_context.get_state(max_descriptor)

        # Running sum state
        sum_descriptor = ValueStateDescriptor("sum", Types.FLOAT())
        sum_descriptor.enable_time_to_live(state_ttl_config)
        self.sum_state = runtime_context.get_state(sum_descriptor)

        # Running sum of squares state
        sum_squares_descriptor = ValueStateDescriptor("sum_squares", Types.FLOAT())
        sum_squares_descriptor.enable_time_to_live(state_ttl_config)
        self.sum_squares_state = runtime_context.get_state(sum_squares_descriptor)

        # Count state
        count_descriptor = ValueStateDescriptor("count", Types.LONG())
        count_descriptor.enable_time_to_live(state_ttl_config)
        self.count_state = runtime_context.get_state(count_descriptor)

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        """Process each element and compute features."""
        key = value[0]
        current_value = float(value[1])
        timestamp = value[2] if len(value) > 2 else ctx.timestamp()

        # Get current state values
        values = list(self.values_state.get()) if self.values_state.get() else []
        current_min = (
            self.min_state.value()
            if self.min_state.value() is not None
            else float("inf")
        )
        current_max = (
            self.max_state.value()
            if self.max_state.value() is not None
            else float("-inf")
        )
        current_sum = (
            self.sum_state.value() if self.sum_state.value() is not None else 0.0
        )
        current_sum_squares = (
            self.sum_squares_state.value()
            if self.sum_squares_state.value() is not None
            else 0.0
        )
        current_count = (
            self.count_state.value() if self.count_state.value() is not None else 0
        )

        # Add new value to rolling window
        values.append(current_value)

        # Maintain window size
        if len(values) > self.window_size:
            # Remove oldest value from aggregates
            oldest_value = values.pop(0)
            current_sum -= oldest_value
            current_sum_squares -= oldest_value**2
        else:
            current_count += 1

        # Update aggregates with new value
        current_sum += current_value
        current_sum_squares += current_value**2
        current_min = min(current_min, current_value)
        current_max = max(current_max, current_value)

        # Update states
        self.values_state.update(values)
        self.min_state.update(current_min)
        self.max_state.update(current_max)
        self.sum_state.update(current_sum)
        self.sum_squares_state.update(current_sum_squares)
        self.count_state.update(current_count)

        # Calculate features
        features = self._calculate_features(
            values,
            current_sum,
            current_sum_squares,
            current_min,
            current_max,
            current_value,
        )

        # Yield feature vector
        yield (
            key,
            {"timestamp": timestamp, "raw_value": current_value, "features": features},
        )

    def _calculate_features(
        self, values, sum_val, sum_squares, min_val, max_val, current_val
    ):
        """Calculate comprehensive feature set."""
        n = len(values)
        if n == 0:
            return {}

        features = {}

        # Basic statistics
        features["mean"] = sum_val / n
        features["min"] = min_val
        features["max"] = max_val
        features["range"] = max_val - min_val

        # Variance and standard deviation
        if n > 1:
            variance = (sum_squares - (sum_val**2 / n)) / (n - 1)
            features["variance"] = max(0, variance)  # Ensure non-negative
            features["std"] = math.sqrt(features["variance"])
            features["cv"] = (
                features["std"] / features["mean"] if features["mean"] != 0 else 0
            )
        else:
            features["variance"] = 0
            features["std"] = 0
            features["cv"] = 0

        # Percentiles
        sorted_values = sorted(values)
        for p in self.percentiles:
            features[f"p{int(p * 100)}"] = self._calculate_percentile(sorted_values, p)

        # Trend features
        if n >= 3:
            features.update(self._calculate_trend_features(values))

        # Position-based features
        features["current_vs_mean"] = current_val - features["mean"]
        features["current_vs_median"] = current_val - features.get(
            "p50", features["mean"]
        )

        # Normalized features
        if features["std"] > 0:
            features["z_score"] = (current_val - features["mean"]) / features["std"]
        else:
            features["z_score"] = 0

        if features["range"] > 0:
            features["min_max_norm"] = (current_val - min_val) / features["range"]
        else:
            features["min_max_norm"] = 0.5

        # Change features
        if n >= 2:
            prev_val = values[-2]
            features["change_abs"] = current_val - prev_val
            features["change_pct"] = (
                (current_val - prev_val) / prev_val if prev_val != 0 else 0
            )

        return features

    def _calculate_percentile(self, sorted_values, percentile):
        """Calculate percentile from sorted values."""
        n = len(sorted_values)
        if n == 0:
            return 0
        if n == 1:
            return sorted_values[0]

        index = percentile * (n - 1)
        lower_index = int(index)
        upper_index = min(lower_index + 1, n - 1)

        if lower_index == upper_index:
            return sorted_values[lower_index]

        # Linear interpolation
        weight = index - lower_index
        return (
            sorted_values[lower_index] * (1 - weight)
            + sorted_values[upper_index] * weight
        )

    def _calculate_trend_features(self, values):
        """Calculate trend-based features using linear regression."""
        n = len(values)
        x = list(range(n))

        # Calculate linear regression slope
        x_mean = sum(x) / n
        y_mean = sum(values) / n

        numerator = sum((x[i] - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))

        trend_features = {}
        if denominator > 0:
            slope = numerator / denominator
            trend_features["slope"] = slope
            trend_features["trend_strength"] = abs(slope)

            # R-squared (coefficient of determination)
            y_pred = [y_mean + slope * (x[i] - x_mean) for i in range(n)]
            ss_res = sum((values[i] - y_pred[i]) ** 2 for i in range(n))
            ss_tot = sum((values[i] - y_mean) ** 2 for i in range(n))
            trend_features["r_squared"] = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
        else:
            trend_features["slope"] = 0
            trend_features["trend_strength"] = 0
            trend_features["r_squared"] = 0

        return trend_features


def generate_sensor_data(num_records):
    """Generate realistic sensor data with trends and noise."""
    sensors = ["temp_sensor_1", "temp_sensor_2", "pressure_sensor_1"]
    base_values = {
        "temp_sensor_1": 22.0,
        "temp_sensor_2": 24.0,
        "pressure_sensor_1": 1013.25,
    }
    trends = {"temp_sensor_1": 0.1, "temp_sensor_2": -0.05, "pressure_sensor_1": -0.2}

    print(f"Generating {num_records} sensor readings...")

    for i in range(num_records):
        sensor = sensors[i % len(sensors)]
        # Base value with trend and noise
        trend_component = trends[sensor] * (i // len(sensors))
        noise = random.gauss(0, 0.5)  # Gaussian noise
        seasonal = 2 * math.sin(2 * math.pi * i / 20)  # Seasonal component

        value = base_values[sensor] + trend_component + noise + seasonal
        timestamp = 1000 + i * 1000  # Mock timestamp

        yield (sensor, value, timestamp)


def feature_engineering_demo():
    """Demo of real-time feature engineering."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # env.from_source()
    env.enable_checkpointing(200)

    # Generate streaming sensor data
    data_generator = generate_sensor_data(num_records=600)

    ds = env.from_collection(
        collection=data_generator,
        type_info=Types.TUPLE([Types.STRING(), Types.FLOAT(), Types.LONG()]),
    ).uid("6knum")

    # Apply feature engineering processor
    ds.key_by(lambda value: value[0]).process(
        FeatureEngineeringProcessor(window_size=8, percentiles=[0.25, 0.5, 0.75, 0.9])
    ).uid("feature-engineering").map(
        lambda x: f"Sensor: {x[0]}, Value: {x[1]['raw_value']:.2f}, "
        f"Mean: {x[1]['features'].get('mean', 0):.2f}, "
        f"Std: {x[1]['features'].get('std', 0):.2f}, "
        f"Z-Score: {x[1]['features'].get('z_score', 0):.2f}, "
        f"Trend: {x[1]['features'].get('slope', 0):.4f}"
    ).print()

    # Execute the pipeline
    env.execute("Real-time Feature Engineering Demo")


if __name__ == "__main__":
    feature_engineering_demo()
