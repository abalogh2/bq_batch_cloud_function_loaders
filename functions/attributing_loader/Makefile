upload_source:
	zip /tmp/attributing_loader_source.zip main.py requirements.txt sales_data_schema.json loader_template1.sql loader_template2.sql
	gsutil cp /tmp/attributing_loader_source.zip gs://bud-gcp-meetup-functions/attributing_loader/

deploy: upload_source
	gcloud functions deploy attributing_loader \
	--project bud-gcp-meetup \
	--memory 128MB \
	--timeout 540s \
	--region europe-west1 \
	--entry-point load_from_gcs \
	--runtime python37 \
	--trigger-resource gs://bud-gcp-meetup-sales-data-attributing-loader \
	--trigger-event google.storage.object.finalize \
	--service-account cloud-function-user@bud-gcp-meetup.iam.gserviceaccount.com \
	--source gs://bud-gcp-meetup-functions/attributing_loader/attributing_loader_source.zip \
	--set-env-vars=SQL_VERSION=1
