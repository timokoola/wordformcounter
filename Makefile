
# upload main.py and requirements.txt to the board using 
# gcloud cloud functions deploy
deploy:
	gcloud functions deploy wordformcounter \
	--gen2 \
	--region=europe-north1 \
	--runtime=python311 \
	--source=. \
	--entry-point=counter 