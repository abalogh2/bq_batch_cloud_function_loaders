upload_source:
	zip /tmp/aggregating_loader_source.zip main.py requirements.txt sales_data_schema.json loader_template.sql
	gsutil cp /tmp/aggregating_loader_source.zip gs://bud-gcp-meetup-functions/aggregating_loader/

deploy: upload_source
	gcloud functions deploy aggregating_loader \
	--project bud-gcp-meetup \
	--memory 128MB \
	--timeout 540s \
	--region europe-west1 \
	--entry-point load_from_gcs \
	--runtime python37 \
	--trigger-resource gs://bud-gcp-meetup-sales-data-aggregating-loader \
	--trigger-event google.storage.object.finalize \
	--service-account cloud-function-user@bud-gcp-meetup.iam.gserviceaccount.com \
	--source gs://bud-gcp-meetup-functions/aggregating_loader/aggregating_loader_source.zip
