package logreader;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit.Callback;
import retrofit.RetrofitError;
import retrofit.client.Response;

public class LogglyCallback implements Callback<Response> {
    private final Logger LOG = LoggerFactory.getLogger(LogglyCallback.class);

    @Override
    public void success(Response response, Response response2) {
        //LOG.info("success");
    }

    @Override
        public void failure(RetrofitError retrofitError) {
        LOG.error(retrofitError.toString());
    }
}
