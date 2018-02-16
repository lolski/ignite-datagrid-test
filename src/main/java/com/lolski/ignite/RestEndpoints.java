package com.lolski.ignite;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static spark.Spark.get;
import static spark.Spark.post;

public class RestEndpoints {
    public static void setupRestEndpoints(Supplier<String> onGetKeys, Consumer<String> onPut, Function<String, String> onGetKey) {
        get("/keys", (req, res) -> onGetKeys.get());

        post("/add", (req, res) -> {
            onPut.accept(req.body());
            return "OK";
        });

        get("/keys/:key", (req, res) -> onGetKey.apply(req.params("key")));
    }
}
