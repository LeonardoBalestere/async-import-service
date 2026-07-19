# Estágio 1: build com o SDK completo (nunca vai para produção)
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY src/ImportService.Contracts/ImportService.Contracts.csproj src/ImportService.Contracts/
COPY src/ImportService.Data/ImportService.Data.csproj src/ImportService.Data/
COPY src/ImportService.Messaging/ImportService.Messaging.csproj src/ImportService.Messaging/
COPY src/ImportService.Worker/ImportService.Worker.csproj src/ImportService.Worker/
RUN dotnet restore src/ImportService.Worker/ImportService.Worker.csproj

COPY src/ src/
RUN dotnet publish src/ImportService.Worker/ImportService.Worker.csproj -c Release -o /app --no-restore

# Estágio 2: runtime chiseled — worker não serve HTTP, basta o runtime base.
FROM mcr.microsoft.com/dotnet/runtime:10.0-noble-chiseled
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "ImportService.Worker.dll"]
