#!/bin/bash
set -euo pipefail

JAR=/opt/flink/usrlib/flight-anomaly-detector-0.1.0-SNAPSHOT-all.jar
JM=jobmanager:8081
JOB_NAME="Flight Anomaly Detector"

echo "Waiting for JobManager at $JM..."
for i in $(seq 1 90); do
  if flink list -m "$JM" &>/dev/null; then
    echo "JobManager is ready."
    break
  fi
  sleep 2
done

if ! flink list -m "$JM" &>/dev/null; then
  echo "ERROR: JobManager not reachable at $JM"
  exit 1
fi

if flink list -m "$JM" 2>/dev/null | grep -q "${JOB_NAME}.*RUNNING"; then
  echo "Job already RUNNING — skipping submit."
  flink list -m "$JM"
  exit 0
fi

echo "Checking for job jar..."
ls -la /opt/flink/usrlib/ || true
if [[ ! -f "$JAR" ]]; then
  echo "ERROR: Missing $JAR — flink-jar service may have failed."
  exit 1
fi

echo "Submitting ${JOB_NAME}..."
flink run -d -m "$JM" -c com.flightanomaly.FlightAnomalyJob "$JAR"

echo "Done."
flink list -m "$JM"
