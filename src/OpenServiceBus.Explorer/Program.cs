using OpenServiceBus.Explorer.Api;
using OpenServiceBus.Explorer.Sessions;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<SessionManager>();
builder.Services.AddHttpClient();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapExplorerEndpoints();

await app.RunAsync();
