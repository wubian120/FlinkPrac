package cn.bw.flink.prac;

public class Host {

    // 主机名
    private String hostname;

    // 端口号
    private int port;

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return hostname + " : " + port;
    }




}
