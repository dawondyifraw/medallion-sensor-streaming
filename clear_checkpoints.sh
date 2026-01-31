#!/bin/bash

echo "🧹 Resetting Medallion architecture..."
echo "🔸 Deleting Bronze layer..."
rm -rf data/bronze/sensor_data_medallion
rm -rf data/bronze/_checkpoints/sensor_data_medallion
echo "⚪ Deleting Silver layer..."
rm -rf data/silver/sensor_data_medallion
rm -rf data/silver/_checkpoints/sensor_data_medallion
echo "🟡 Deleting Gold layer..."
rm -rf data/gold/sensor_data_medallion
rm -rf data/gold/_checkpoints/zone_hourly_summary
echo "checkpoints cleared."
