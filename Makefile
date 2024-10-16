.PHONY: test server reload
server:
	flask --app server --debug run
test:
	python -m unittest
reload:
	echo kill -SIGHUP $$(cat /tmp/tfmpid) | bash
