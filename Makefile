.PHONY: test server reload
server:
	flask --app src/server/server --debug run
test:
	python -m unittest discover -s tests
reload:
	echo kill -SIGHUP $$(cat /tmp/tfmpid) | bash
