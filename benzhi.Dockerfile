# 官方 Go 镜像（daocloud 镜像，与生成机 Go 1.26.3 完全一致），自带完整工具链
FROM docker.m.daocloud.io/library/golang:1.26.3-bookworm

WORKDIR /app

ENV CGO_ENABLED=0 \
    GOTOOLCHAIN=local \
    GOPROXY=https://goproxy.cn,direct \
    GOSUMDB=sum.golang.google.cn

# 先复制依赖文件并下载依赖，利用 Docker 缓存并保证容器内可用
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# 预编译一次，把编译缓存留在镜像里；不影响模型修改源码
RUN go build ./...

# 容器启动后进入 shell，方便操作
CMD ["bash"]
