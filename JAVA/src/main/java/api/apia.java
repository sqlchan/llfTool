package api;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("aaa")
@Produces({MediaType.APPLICATION_JSON})
@Consumes({MediaType.APPLICATION_JSON})
public interface apia {
    @GET
    @Path("/info")
    String getInfo();

    @POST
    @Path("/geys")
    String getKeys(String cmKeys);
}
