up:
	docker-compose up -d --build

down:
	docker-compose down

exec:
	docker exec -it crawlers sh

clean:
	rm -r $(path)/data
	rm -r $(path)/logs
