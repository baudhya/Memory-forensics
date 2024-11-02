docker build -t my_postgres .
docker run -d -p 5432:5432 --name postgres_container my_postgres &
