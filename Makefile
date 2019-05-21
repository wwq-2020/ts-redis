docker-test:
	@docker-compose down
	@docker-compose up

local-test:
	deno -A test.ts


