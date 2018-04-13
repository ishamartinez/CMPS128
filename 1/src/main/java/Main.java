import spark.Spark;

public class Main {

    public static void main(String[] args) {

        Spark.port(8080);

        Spark.get("/hello", (request, response) -> {

            response.type("text/plain");
            response.status(200);
            response.body("Hello world!");

            return response.body();
        });

        Spark.get("/echo", (request, response) -> {

            String msg = request.queryParams("msg");
            msg = msg != null ? msg : "";

            response.type("text/plain");
            response.status(200);
            response.body(msg);

            return response.body();
        });
    }
}
