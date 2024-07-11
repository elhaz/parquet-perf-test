venv/Scripts/Activate;
celery -A celery_tasks worker `
-l info `
-P gevent `
-n wr@%h `
-c 1 `
-Q db_write `
;