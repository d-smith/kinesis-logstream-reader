package loggly;


import retrofit.Callback;
import retrofit.client.Response;
import retrofit.http.Body;
import retrofit.http.POST;
import retrofit.http.Path;

import javax.annotation.PostConstruct;

public interface LogglyService {
    @POST("/inputs/{token}/tag/b2bnext")
    void postLogData(@Path("token") String token, @Body LogMessage logData, Callback<Response> cb);
}
