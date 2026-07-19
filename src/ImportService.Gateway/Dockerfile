# Estágio 1: build com o SDK completo (nunca vai para produção)
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Só os .csproj primeiro: o restore vira camada cacheada — mudar código-fonte
# não invalida o download de pacotes.
COPY src/ImportService.Contracts/ImportService.Contracts.csproj src/ImportService.Contracts/
COPY src/ImportService.Data/ImportService.Data.csproj src/ImportService.Data/
COPY src/ImportService.Messaging/ImportService.Messaging.csproj src/ImportService.Messaging/
COPY src/ImportService.Gateway/ImportService.Gateway.csproj src/ImportService.Gateway/
RUN dotnet restore src/ImportService.Gateway/ImportService.Gateway.csproj

COPY src/ src/
RUN dotnet publish src/ImportService.Gateway/ImportService.Gateway.csproj -c Release -o /app --no-restore

# Estágio 2: runtime chiseled — sem shell, sem package manager, non-root.
FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled
# Vincula o pacote no GHCR a este repositório (acesso do CI e página do pacote).
LABEL org.opencontainers.image.source=https://github.com/LeonardoBalestere/async-import-service
WORKDIR /app
COPY --from=build /app .
# A imagem base já expõe 8080 e roda como usuário não-privilegiado (app).
ENTRYPOINT ["dotnet", "ImportService.Gateway.dll"]
