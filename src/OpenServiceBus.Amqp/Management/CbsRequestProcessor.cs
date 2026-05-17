using Amqp;
using Amqp.Framing;
using Amqp.Listener;

namespace OpenServiceBus.Amqp.Management;

/// <summary>
/// Handles AMQP Claims-Based Security ($cbs) <c>put-token</c> requests.
///
/// In emulator mode (the OpenServiceBus default), this processor accepts any token
/// and replies with statusCode=202 / statusDescription="Accepted". Real SAS validation
/// is opt-in and arrives in M9.
///
/// Critical wire contract (the Azure SDK is strict, fails silently otherwise):
/// <list type="bullet">
///   <item>Response <c>properties.correlation-id</c> must echo the request's <c>properties.message-id</c>.</item>
///   <item>Response <c>application-properties</c> keys are <b>camelCase</b> (<c>statusCode</c>, <c>statusDescription</c>).</item>
///   <item>Status 202 (Accepted) on success.</item>
/// </list>
/// </summary>
public sealed class CbsRequestProcessor : IRequestProcessor
{
    /// <summary>Link credit advertised to clients on the $cbs receiver link.</summary>
    public int Credit => 100;

    public void Process(RequestContext requestContext)
    {
        var request = requestContext.Message;

        var response = BuildAcceptedResponse(request);
        requestContext.Complete(response);
    }

    private static Message BuildAcceptedResponse(Message request)
    {
        var response = new Message
        {
            Properties = new Properties(),
            ApplicationProperties = new ApplicationProperties(),
        };

        var requestMessageId = request.Properties?.GetMessageId();
        if (requestMessageId is not null)
        {
            response.Properties.SetCorrelationId(requestMessageId);
        }

        response.ApplicationProperties["statusCode"] = 202;
        response.ApplicationProperties["statusDescription"] = "Accepted";

        return response;
    }
}
