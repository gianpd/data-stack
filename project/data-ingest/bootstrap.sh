echo "Data-Ingest container started ..."
echo "Application: $APPLICATION"

export ROOT_FOLDER=$(pwd)
export list_dir=$(ls)
echo " - ROOT_FOLDER:    "$ROOT_FOLDER
echo " - List dir:  "$list_dir

if [ $APPLICATION == "zip-events" ]; then
  cd app
  python asynch_launcher.py apply-daily-lifecycle
fi
if [ $APPLICATION == "start-server" ]; then
  uvicorn app.main:app --reload --workers 1 --host 0.0.0.0 --port 5010
fi