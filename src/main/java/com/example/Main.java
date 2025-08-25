package com.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;

public class Main {
    private static InetSocketAddress mapped = null;
    public static void main(String[] args) throws IOException, InterruptedException {
        InetSocketAddress stun = new InetSocketAddress(
            System.getProperty("stun.server.addr", "stun.l.google.com"),
            Integer.getInteger("stun.server.port", 19302)
        );
        try (UdpConnectionManager manager = new UdpConnectionManager();) {
            manager.setKeepAliveInterval(
                3000
            ).setKeepAliveData(
                stun, () -> SimpleStun.generateRequest()
            ).onKeepAliveTimeout(addr -> {
                System.out.println(String.format(
                    "%tT [onKeepAliveTimeout] %s:%d retry...",
                    new Date(),
                    addr.getAddress().getHostAddress(),
                    addr.getPort()
                ));
            }).onDisconnect(addr -> {
                System.out.println(String.format(
                    "%tT [onDisconnect] %s:%d retry...",
                    new Date(),
                    addr.getAddress().getHostAddress(),
                    addr.getPort()
                ));
            }).onReceive(stun, bytes -> {
                // TODO: 汚い
                try {
                    InetSocketAddress response = SimpleStun.parseResponse(ByteBuffer.wrap(bytes));
                    if (mapped == null) {
                        if (response != null) {
                            mapped = response;
                            System.out.println(String.format(
                                "your mapped addr = %s:%d",
                                response.getAddress().getHostAddress(),
                                response.getPort()
                            ));
                            System.out.println(String.format(
                                "pleas input peer addr: "
                            ));
                            try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in));) {
                                String line = input.readLine();
                                InetSocketAddress peer = Utils.parseAddress(line);
                                if (peer != null) {
                                    manager.setKeepAliveData(
                                        peer, () -> ByteBuffer.allocate(0)
                                    ).setKeepAliveInterval(
                                        peer, 100
                                    ).onConnect(peer, () -> {
                                        System.out.println(String.format(
                                            "%tT [onConnect] %s:%d",
                                            new Date(),
                                            peer.getAddress().getHostAddress(),
                                            peer.getPort()
                                        ));
                                    }).onReceive(
                                        peer, data -> {
                                            if (data.length > 0) {
                                                String message = new String(data);
                                                System.out.println(String.format(
                                                    "%tT [onReceive] %s:%d %s",
                                                    new Date(),
                                                    peer.getAddress().getHostAddress(),
                                                    peer.getPort(),
                                                    message
                                                ));
                                            }
                                        }
                                    ).connect(peer);
                                }
                            } catch (IOException|InterruptedException e1) {
                                e1.printStackTrace();
                            }
                        } else {
                            System.out.println("stun parse error.");
                        }
                    }
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
            }).onError(entry -> {
                System.err.println(String.format(
                    "%tT [onError] %s:%d %s(%s)",
                    new Date(),
                    entry.getKey().getAddress().getHostAddress(),
                    entry.getKey().getPort(),
                    entry.getValue().getClass().getName(),
                    entry.getValue().getMessage()
                ));
            }).onGlobalError(e -> {
                System.err.println(String.format(
                    "%tT [onError] %s(%s)",
                    new Date(),
                    e.getClass().getName(),
                    e.getMessage()
                ));
            }).start();

            manager.connect(stun);

            while (true) {
                for (InetSocketAddress addr: manager.getActiveConnections()) {
                    // TODO: 分けて管理
                    if (!stun.equals(addr)) {
                        manager.send(new Date().toString().getBytes(Charset.defaultCharset()), addr);
                    }
                }
                Thread.sleep(1000);
            }
        }
    }
}
