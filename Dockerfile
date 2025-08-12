FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080
EXPOSE 8081

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Rinha2025.csproj", "./"]
RUN dotnet restore "Rinha2025.csproj"
COPY . .
WORKDIR "/src/"
RUN dotnet build "./Rinha2025.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./Rinha2025.csproj" -c $BUILD_CONFIGURATION -r linux-x64 -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
#ENTRYPOINT ["dotnet", "Rinha2025.dll"]
ENTRYPOINT ["./Rinha2025"]
