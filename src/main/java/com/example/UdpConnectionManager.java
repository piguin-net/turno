package com.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class UdpConnectionManager implements AutoCloseable
{
    private DatagramChannel channel;
    private Thread receiver;
    private boolean active = false;
    private List<Consumer<Entry<InetSocketAddress, byte[]>>> receiveEventListener = new ArrayList<>();
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
        private List<Consumer<UdpConnection>> keepaliveTimeoutEventListener = new ArrayList<>();
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

        public boolean isActive() {
            return this.active;
        }

        public boolean isKeepAliveTimeout() {
            return new Date().getTime() - this.last > this.keepaliveTimeout;
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
        public UdpConnection onKeepAliveTimeout(Consumer<UdpConnection> listener) {
            this.keepaliveTimeoutEventListener.add(listener);
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
                            if (this.last > 0 && this.isKeepAliveTimeout()) {
                                this.last = -1 * new Date().getTime();
                                this.keepaliveTimeoutEventListener.forEach(
                                    listener -> new Thread(
                                        () -> listener.accept(this),
                                        String.format(
                                            "UdpConnectionManager KeepaliveTimeoutEventListenerThread(%s:%d)",
                                            this.addr.getAddress().getHostAddress(),
                                            this.addr.getPort()
                                        )
                                    ).start()
                                );
                            }
                            try {
                                this.manager.channel.send(this.generator.get(), this.addr);
                            } catch (Exception e) {
                                if (this.active) {
                                    this.error(e);
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
            this.keepalive.join();
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
        }
    }

    public UdpConnectionManager() throws IOException {
        this.channel = DatagramChannel.open();
        this.channel.configureBlocking(false);
    }

    public UdpConnectionManager setPort(int port) throws SocketException {
        this.channel.socket().bind(new InetSocketAddress(port));
        return this;
    }

    public UdpConnectionManager joinGroup(InetAddress group) throws UnknownHostException, IOException {
        for (NetworkInterface nic: getSiteLocalNetworkInterfaces().keySet()) {
            this.channel.join(group, nic);
        }
        return this;
    }

    public UdpConnectionManager onReceive(Consumer<Entry<InetSocketAddress, byte[]>> listener) {
        this.receiveEventListener.add(listener);
        return this;
    }

    public UdpConnectionManager onError(Consumer<Exception> listener) {
        this.errorEventListener.add(listener);
        return this;
    }

    public UdpConnection newConnection(InetSocketAddress addr) {
        return new UdpConnection(this, addr);
    }

    public List<UdpConnection> getAllConnections() {
        return new ArrayList<>(this.peers.values());
    }

    public List<UdpConnection> getActiveConnections() {
        return this.peers.values().stream().filter(
            peer -> peer.isActive()
        ).filter(
            peer -> peer.last() > 0
        ).filter(
            peer -> !peer.isKeepAliveTimeout()
        ).toList();
    }

    public UdpConnectionManager start() {
        this.active = true;
        this.receiver = new Thread(() -> {
            while (this.active) {
                try {
                    ByteBuffer buffer = ByteBuffer.allocate(65535);
                    SocketAddress socket = this.channel.receive(buffer);
                    if (socket instanceof InetSocketAddress addr) {
                        byte[] data = new byte[buffer.flip().limit()];
                        buffer.get(data);
                        if (peers.containsKey(addr)) {
                            peers.get(addr).receive(data);
                        } else {
                            // TODO: 送信元ではなく送信先アドレスを知るすべが無いか
                            // group宛なのか、firewallが許可されてて知らない相手から届いたのか、
                            // 判断できない
                            this.receiveEventListener.forEach(
                                listener -> new Thread(
                                    () -> listener.accept(Map.entry(addr, data)),
                                    "UdpConnectionManager ErrorEventListenerThread"
                                ).start()
                            );
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

    private static Map<NetworkInterface, List<String>> getSiteLocalNetworkInterfaces() throws SocketException {
        Map<NetworkInterface, List<String>> nics = new HashMap<>();
        for (NetworkInterface nic: Collections.list(NetworkInterface.getNetworkInterfaces())) {
            for (InterfaceAddress addr: nic.getInterfaceAddresses()) {
                if (addr.getAddress().isSiteLocalAddress()) {
                    if (!nics.containsKey(nic)) nics.put(nic, new ArrayList<>());
                    nics.get(nic).add(addr.getAddress().getHostAddress());
                }
            }
        }
        return nics;
    }

    public static void main( String[] args ) throws Exception
    {
        Integer port = Integer.getInteger("p2p.port", 9625);
        Integer interval = Integer.getInteger("p2p.keepalive.interval", 1000);

        try (UdpConnectionManager manager = new UdpConnectionManager();) {
            manager.setPort(
                port
            ).joinGroup(
                InetAddress.getByName("224.0.0.1")
            ).onReceive(entry -> {
                System.out.println(String.format(
                    "%tT [onReceive] %s:%d",
                    new Date(),
                    entry.getKey().getAddress().getHostAddress(),
                    entry.getKey().getPort()
                ));
            }).onError((e) -> {
                System.out.println(String.format(
                    "%tT [onError] %s(%s)",
                    new Date(),
                    e.getClass().getName(),
                    e.getMessage()
                ));
            }).start();

            for (String arg: args) {
                InetSocketAddress addr = new InetSocketAddress(arg, port);
                manager.newConnection(addr).setKeepAliveInterval(
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
                }).onKeepAliveTimeout(instance -> {
                    System.out.println(String.format(
                        "%tT [onKeepAliveTimeout] %s:%d retry...",
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
                    } else {
                        // keepalive
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
                    for (UdpConnection connection: manager.getActiveConnections()) {
                        connection.send(message.getBytes(Charset.defaultCharset()));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
