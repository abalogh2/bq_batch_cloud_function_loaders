upload_source:
	zip /tmp/simple_loader_source.zip main.py requirements.txt
	gsutil cp /tmp/simple_loader_source.zip gs://bud-gcp-meetup-functions/simple_loader/

deploy: upload_source
	gcloud functions deploy simple_loader \
	--project bud-gcp-meetup \
	--memory 128MB \
	--timeout 540s \
	--region europe-west1 \
	--entry-point load_from_gcs \
	--runtime python37 \
	--trigger-resource gs://bud-gcp-meetup-sales-data-simple-loader \
	--trigger-event google.storage.object.finalize \
	--service-account cloud-function-user@bud-gcp-meetup.iam.gserviceaccount.com \
	--source gs://bud-gcp-meetup-functions/simple_loader/simple_loader_source.zip

