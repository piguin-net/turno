package com.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class UdpConnectionManager implements AutoCloseable
{
    private DatagramChannel channel;
    private Thread receiver;
    private boolean active = false;
    private List<Consumer<Exception>> errorEventListener = new ArrayList<>();
    private Map<InetSocketAddress, UdpConnection> peers = new HashMap<>();

    public static class UdpConnection implements AutoCloseable {
        private final UdpConnectionManager manager;
        private final InetSocketAddress addr;
        private boolean active = false;
        private Thread keepalive;
        private int interval = 1_000;
        private Supplier<ByteBuffer> generator = () -> ByteBuffer.allocate(0);
        // private int connectionTimeout = 3 * 60_000;
        private int keepaliveTimeout = 3_000;
        private long last = -1 * new Date().getTime();

        private List<Consumer<UdpConnection>> connectEventListener = new ArrayList<>();
        private List<Consumer<UdpConnection>> disconnectEventListener = new ArrayList<>();
        private List<Consumer<byte[]>> receiveEventListener = new ArrayList<>();
        private List<Consumer<Exception>> errorEventListener = new ArrayList<>();

        public UdpConnection(UdpConnectionManager manager, InetSocketAddress addr) {
            this.manager = manager;
            this.addr = addr;
        }

        public long last() {
            return this.last;
        }

        public void send(byte[] data) throws IOException, InterruptedException {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            this.connect();
            this.manager.channel.send(buffer, this.addr);
        }

        public UdpConnection setKeepAliveInterval(int interval) {
            this.interval = interval;
            return this;
        }
        public UdpConnection setKeepAliveData(Supplier<ByteBuffer> generator) {
            this.generator = generator;
            return this;
        }
        public UdpConnection onConnect(Consumer<UdpConnection> listener) {
            this.connectEventListener.add(listener);
            return this;
        }
        public UdpConnection onDisconnect(Consumer<UdpConnection> listener) {
            this.disconnectEventListener.add(listener);
            return this;
        }
        public UdpConnection onReceive(Consumer<byte[]> listener) {
            this.receiveEventListener.add(listener);
            return this;
        }
        public UdpConnection onError(Consumer<Exception> listener) {
            this.errorEventListener.add(listener);
            return this;
        }

        public void connectAsync() throws InterruptedException {
            if (this.manager.peers.containsKey(this.addr)) {
                if (this.manager.peers.get(this.addr) == this) {
                    return;
                } else {
                    this.manager.peers.get(this.addr).close();
                }
            }
            this.manager.peers.put(this.addr, this);
            this.last = -1 * new Date().getTime();
            this.active = true;
            this.keepalive = new Thread(
                () -> {
                    while (this.active) {
                        try {
                            long now = new Date().getTime();
                            if (this.last > 0 && now - this.last > this.keepaliveTimeout) {
                                this.disconnect();
                            } else {
                                try {
                                    this.manager.channel.send(this.generator.get(), this.addr);
                                } catch (Exception e) {
                                    if (this.active) {
                                        this.error(e);
                                    }
                                }
                            }
                            Thread.sleep(this.interval);
                        } catch (InterruptedException e) {
                            if (this.active) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                },
                String.format(
                    "UdpConnectionManager KeepAliveThread(%s:%d)",
                    this.addr.getAddress().getHostAddress(),
                    this.addr.getPort()
                )
            );
            this.keepalive.start();
        }

        public void connect() throws InterruptedException {
            this.connectAsync();
            while (this.last < 0) {
                Thread.sleep(100);
            }
        }

        public void disconnect() throws InterruptedException {
            this.manager.peers.remove(this.addr);
            this.active = false;
            this.disconnectEventListener.forEach(
                listener -> new Thread(
                    () -> listener.accept(this),
                    String.format(
                        "UdpConnectionManager DisconnectEventListenerThread(%s:%d)",
                        this.addr.getAddress().getHostAddress(),
                        this.addr.getPort()
                    )
                ).start()
            );
        }

        private void receive(byte[] data) {
            if (this.last < 0) {
                this.connectEventListener.forEach(
                    listener -> new Thread(
                        () -> listener.accept(this),
                        String.format(
                            "UdpConnectionManager ConnectEventListenerThread(%s:%d)",
                            this.addr.getAddress().getHostAddress(),
                            this.addr.getPort()
                        )
                    ).start()
                );
            }
            this.last = new Date().getTime();
            this.receiveEventListener.forEach(
                listener -> new Thread(
                    () -> listener.accept(data),
                    String.format(
                        "UdpConnectionManager ReceiveEventListenerThread(%s:%d)",
                        this.addr.getAddress().getHostAddress(),
                        this.addr.getPort()
                    )
                ).start()
            );
        }

        private void error(Exception e) {
            this.errorEventListener.forEach(
                listener -> new Thread(
                    () -> listener.accept(e),
                    String.format(
                        "UdpConnectionManager ErrorEventListenerThread(%s:%d)",
                        this.addr.getAddress().getHostAddress(),
                        this.addr.getPort()
                    )
                ).start()
            );
        }

        @Override
        public void close() throws InterruptedException {
            this.disconnect();
            this.keepalive.join();
        }
    }

    public UdpConnectionManager(Integer port) throws IOException {
        this.channel = DatagramChannel.open();
        this.channel.configureBlocking(false);
        this.channel.socket().bind(new InetSocketAddress(port));
    }

    public UdpConnectionManager onError(Consumer<Exception> listener) {
        this.errorEventListener.add(listener);
        return this;
    }

    public UdpConnection newConnection(InetSocketAddress addr) {
        return new UdpConnection(this, addr);
    }

    public List<UdpConnection> getConnections() {
        return new ArrayList<>(this.peers.values());
    }

    public UdpConnectionManager start() {
        this.active = true;
        this.receiver = new Thread(() -> {
            while (this.active) {
                try {
                    ByteBuffer buffer = ByteBuffer.allocate(65535);
                    SocketAddress socket = this.channel.receive(buffer);
                    if (socket instanceof InetSocketAddress addr) {
                        // TODO: 知らない接続元
                        if (!peers.containsKey(addr)) {
                            new UdpConnection(this, addr).connectAsync();
                        }
                        if (buffer.flip().limit() > 0) {
                            byte[] data = new byte[buffer.limit()];
                            buffer.get(data);
                            peers.get(addr).receive(data);
                        } else {
                            peers.get(addr).receive(new byte[]{});
                        }
                    }
                    Thread.sleep(1);
                } catch (Exception e) {
                    if (this.active) {
                        this.errorEventListener.forEach(
                            listener -> new Thread(
                                () -> listener.accept(e),
                                "UdpConnectionManager ErrorEventListenerThread"
                            ).start()
                        );
                    }
                }
            }
        }, "UdpConnectionManager ReceiverThread");
        this.receiver.start();
        return this;
    }

    @Override
    public void close() throws Exception {
        this.active = false;
        this.receiver.join();
        for (UdpConnection peer: this.peers.values()) {
            peer.close();
        }
        this.channel.close();
    }

    public static void main( String[] args ) throws Exception
    {
        Integer port = Integer.getInteger("p2p.port", 9625);
        Integer interval = Integer.getInteger("p2p.keepalive.interval", 1000);

        try (UdpConnectionManager manager = new UdpConnectionManager(port);) {
            manager.onError((e) -> {
                System.out.println(String.format(
                    "%tT [onError] %s(%s)",
                    new Date(),
                    e.getClass().getName(),
                    e.getMessage()
                ));
            });
            manager.start();

            List<UdpConnection> connections = new ArrayList<>();

            for (String arg: args) {
                InetSocketAddress addr = new InetSocketAddress(arg, port);
                UdpConnection connection = manager.newConnection(addr);
                connections.add(connection);
                connection.setKeepAliveInterval(
                    interval
                ).setKeepAliveData(
                    () -> ByteBuffer.allocate(0)
                ).onConnect(instance -> {
                    System.out.println(String.format(
                        "%tT [onConnect] %s:%d",
                        new Date(),
                        addr.getAddress().getHostAddress(),
                        addr.getPort()
                    ));
                }).onDisconnect(instance -> {
                    System.out.println(String.format(
                        "%tT [onDisconnect] %s:%d retry...",
                        new Date(),
                        addr.getAddress().getHostAddress(),
                        addr.getPort()
                    ));
                    try {
                        // TODO: KeepAliveThreadが増殖している
                        instance.connect();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }).onReceive(data -> {
                    if (data.length > 0) {
                        String message = new String(data);
                        System.out.println(String.format(
                            "%tT [onReceive] %s:%d %s",
                            new Date(),
                            addr.getAddress().getHostAddress(),
                            addr.getPort(),
                            message
                        ));
                    }
                }).onError(error -> {
                    System.out.println(String.format(
                        "%tT [onError] %s:%d %s(%s)",
                        new Date(),
                        addr.getAddress().getHostAddress(),
                        addr.getPort(),
                        error.getClass().getName(),
                        error.getMessage()
                    ));
                }).connect();
            }

            try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in));) {
                while (true) {
                    String message = input.readLine();
                    if (message == null || "".equals(message.trim())) break;
                    for (UdpConnection connection: connections) {
                        connection.send(message.getBytes(Charset.defaultCharset()));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
