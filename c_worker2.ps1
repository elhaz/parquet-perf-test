venv/Scripts/Activate;
celery -A celery_tasks worker `
-l info `
-P gevent `
-n w2@%h `
-c 1 `
# -Q first `
;