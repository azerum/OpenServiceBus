using System.Reflection;
using Amqp.Listener;
using Amqp.Sasl;
using Amqp.Types;

namespace OpenServiceBus.Amqp.Hosting;

/// <summary>
/// Enables the SASL mechanisms an Azure Service Bus client expects:
///   <c>ANONYMOUS</c>  — generic AMQP clients use this.
///   <c>MSSBCBS</c>    — Azure SDK / Service Bus protocol stack always uses this in emulator mode;
///                       at the SASL layer it is a no-op handshake (real auth happens via $cbs put-token).
/// </summary>
internal static class ServiceBusSasl
{
    private const string MssbcbsMechanism = "MSSBCBS";

    public static void ConfigureListenerMechanisms(ConnectionListener listener)
    {
        listener.SASL.EnableAnonymousMechanism = true;
        var mssbcbsProfile = CreateMssbcbsProfile();
        listener.SASL.EnableMechanism(new Symbol(MssbcbsMechanism), mssbcbsProfile);
    }

    /// <summary>
    /// AMQPNetLite ships <c>SaslNoActionProfile</c> as the impl behind <c>SaslProfile.Anonymous</c>
    /// but keeps it <c>internal</c>. Construct it via reflection with our own mechanism name so the
    /// listener advertises MSSBCBS as a supported, no-op mechanism — same wire behavior, different name.
    /// </summary>
    private static SaslProfile CreateMssbcbsProfile()
    {
        var profileType = typeof(SaslProfile).Assembly.GetType("Amqp.Sasl.SaslNoActionProfile", throwOnError: true)!;
        var ctor = profileType.GetConstructor(
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null,
            types: [typeof(string), typeof(string)],
            modifiers: null)
            ?? throw new InvalidOperationException(
                "AMQPNetLite SaslNoActionProfile(string, string) constructor not found - upstream API may have changed.");

        return (SaslProfile)ctor.Invoke([MssbcbsMechanism, null]);
    }
}
