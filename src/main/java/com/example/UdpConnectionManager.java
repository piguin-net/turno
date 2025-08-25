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

        private List<Consumer<InetSocketAddress>> connectEventListener = new ArrayList<>();
        private List<Consumer<InetSocketAddress>> keepaliveTimeoutEventListener = new ArrayList<>();
        private List<Consumer<InetSocketAddress>> disconnectEventListener = new ArrayList<>();
        private List<Consumer<Entry<InetSocketAddress, byte[]>>> receiveEventListener = new ArrayList<>();
        private List<Consumer<Entry<InetSocketAddress, Exception>>> errorEventListener = new ArrayList<>();

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
        public UdpConnection setKeepAliveTimeout(int timeout) {
            this.keepaliveTimeout = timeout;
            return this;
        }
        public UdpConnection setKeepAliveData(Supplier<ByteBuffer> generator) {
            this.generator = generator;
            return this;
        }
        public UdpConnection onConnect(Consumer<InetSocketAddress> listener) {
            this.connectEventListener.add(listener);
            return this;
        }
        public UdpConnection onKeepAliveTimeout(Consumer<InetSocketAddress> listener) {
            this.keepaliveTimeoutEventListener.add(listener);
            return this;
        }
        public UdpConnection onDisconnect(Consumer<InetSocketAddress> listener) {
            this.disconnectEventListener.add(listener);
            return this;
        }
        public UdpConnection onReceive(Consumer<Entry<InetSocketAddress, byte[]>> listener) {
            this.receiveEventListener.add(listener);
            return this;
        }
        public UdpConnection onError(Consumer<Entry<InetSocketAddress, Exception>> listener) {
            this.errorEventListener.add(listener);
            return this;
        }
        private void dispatchConnectEventListener(InetSocketAddress addr) {
            this.connectEventListener.forEach(
                listener -> new Thread(
                    () -> listener.accept(addr),
                    String.format(
                        "UdpConnectionManager ConnectEventListenerThread(%s:%d)",
                        addr.getAddress().getHostAddress(),
                        addr.getPort()
                    )
                ).start()
            );
        }
        private void dispatchKeepAliveTimeoutEventListener(InetSocketAddress addr) {
            this.keepaliveTimeoutEventListener.forEach(
                listener -> new Thread(
                    () -> listener.accept(addr),
                    String.format(
                        "UdpConnectionManager KeepaliveTimeoutEventListenerThread(%s:%d)",
                        addr.getAddress().getHostAddress(),
                        addr.getPort()
                    )
                ).start()
            );
        }
        private void dispatchDisconnectEventListener(InetSocketAddress addr) {
            this.disconnectEventListener.forEach(
                listener -> new Thread(
                    () -> listener.accept(addr),
                    String.format(
                        "UdpConnectionManager DisconnectEventListenerThread(%s:%d)",
                        addr.getAddress().getHostAddress(),
                        addr.getPort()
                    )
                ).start()
            );
        }
        private void dispatchReceiveEventListener(InetSocketAddress addr, byte[] data) {
            this.receiveEventListener.forEach(
                listener -> new Thread(
                    () -> listener.accept(Map.entry(addr, data)),
                    String.format(
                        "UdpConnectionManager ReceiveEventListenerThread(%s:%d)",
                        addr.getAddress().getHostAddress(),
                        addr.getPort()
                    )
                ).start()
            );
        }
        private void dispatchErrorEventListener(InetSocketAddress addr, Exception e) {
            this.errorEventListener.forEach(
                listener -> new Thread(
                    () -> listener.accept(Map.entry(addr, e)),
                    String.format(
                        "UdpConnectionManager ErrorEventListenerThread(%s:%d)",
                        addr.getAddress().getHostAddress(),
                        addr.getPort()
                    )
                ).start()
            );
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
                                this.dispatchKeepAliveTimeoutEventListener(this.addr);
                            }
                            try {
                                this.manager.channel.send(this.generator.get(), this.addr);
                            } catch (Exception e) {
                                if (this.active) {
                                    this.dispatchErrorEventListener(this.addr, e);
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
            this.dispatchDisconnectEventListener(this.addr);
        }

        private void receive(byte[] data) {
            if (this.last < 0) {
                this.dispatchConnectEventListener(this.addr);
            }
            this.last = new Date().getTime();
            this.dispatchReceiveEventListener(this.addr, data);
        }

        @Override
        public void close() throws InterruptedException {
            this.disconnect();
        }
    }

    private DatagramChannel channel;
    private Thread receiver;
    private boolean active = false;
    private Map<InetSocketAddress, UdpConnection> peers = new HashMap<>();
    private List<Consumer<Entry<InetSocketAddress, byte[]>>> receiveEventListener = new ArrayList<>();
    private List<Consumer<Exception>> errorEventListener = new ArrayList<>();

    public UdpConnectionManager() throws IOException {
        this.channel = DatagramChannel.open();
        this.channel.configureBlocking(false);
    }

    public UdpConnectionManager setPort(int port) throws SocketException {
        this.channel.socket().bind(new InetSocketAddress(port));
        return this;
    }

    public int getPort() {
        return this.channel.socket().getLocalPort();
    }

    public UdpConnection joinGroup(InetAddress group) throws UnknownHostException, IOException {
        for (NetworkInterface nic: getSiteLocalNetworkInterfaces().keySet()) {
            this.channel.join(group, nic);
        }
        UdpConnection connection = this.newConnection(new InetSocketAddress(group, this.getPort()));
        connection.last = Long.MAX_VALUE;  // TODO: 汚い
        return connection;
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

    public UdpConnectionManager onReceive(Consumer<Entry<InetSocketAddress, byte[]>> listener) {
        this.receiveEventListener.add(listener);
        return this;
    }

    public UdpConnectionManager onError(Consumer<Exception> listener) {
        this.errorEventListener.add(listener);
        return this;
    }

    private void dispatchReceiveEventListener(InetSocketAddress addr, byte[] data) {
        this.receiveEventListener.forEach(
            listener -> new Thread(
                () -> listener.accept(Map.entry(addr, data)),
                String.format(
                    "UdpConnectionManager ReceiveEventListenerThread(%s:%d)",
                    addr.getAddress().getHostAddress(),
                    addr.getPort()
                )
            ).start()
        );
    }

    private void dispatchErrorEventListener(Exception e) {
        this.errorEventListener.forEach(
            listener -> new Thread(
                () -> listener.accept(e),
                "UdpConnectionManager ErrorEventListenerThread"
            ).start()
        );
    }

    public UdpConnection newConnection(InetSocketAddress addr) {
        return new UdpConnection(this, addr);
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
                            boolean self = getSiteLocalNetworkInterfaces().values().stream().anyMatch(
                                addrs -> addrs.contains(addr.getAddress().getHostAddress())
                            );
                            if (!self) {
                                this.dispatchReceiveEventListener(addr, data);
                            }
                        }
                    }
                    Thread.sleep(1);
                } catch (Exception e) {
                    if (this.active) {
                        this.dispatchErrorEventListener(e);
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

        try (
            UdpConnectionManager manager = new UdpConnectionManager();
            UdpConnectionManager multicast = new UdpConnectionManager();
        ) {
            manager.onError((e) -> {
                System.out.println(String.format(
                    "%tT [onError] %s(%s)",
                    new Date(),
                    e.getClass().getName(),
                    e.getMessage()
                ));
            }).start();

            Consumer<InetSocketAddress> connect = (addr) -> {
                try {
                    manager.newConnection(
                        addr
                    ).setKeepAliveInterval(
                        interval
                    ).setKeepAliveTimeout(
                        interval * 3
                    ).setKeepAliveData(
                        () -> ByteBuffer.allocate(0)
                    ).onConnect(peer -> {
                        System.out.println(String.format(
                            "%tT [onConnect] %s:%d",
                            new Date(),
                            peer.getAddress().getHostAddress(),
                            peer.getPort()
                        ));
                    }).onKeepAliveTimeout(peer -> {
                        System.out.println(String.format(
                            "%tT [onKeepAliveTimeout] %s:%d retry...",
                            new Date(),
                            peer.getAddress().getHostAddress(),
                            peer.getPort()
                        ));
                    }).onDisconnect(peer -> {
                        System.out.println(String.format(
                            "%tT [onDisconnect] %s:%d retry...",
                            new Date(),
                            peer.getAddress().getHostAddress(),
                            peer.getPort()
                        ));
                    }).onReceive(entry -> {
                        if (entry.getValue().length > 0) {
                            String message = new String(entry.getValue());
                            System.out.println(String.format(
                                "%tT [onReceive] %s:%d %s",
                                new Date(),
                                entry.getKey().getAddress().getHostAddress(),
                                entry.getKey().getPort(),
                                message
                            ));
                        } else {
                            // keepalive
                        }
                    }).onError(entry -> {
                        System.out.println(String.format(
                            "%tT [onError] %s:%d %s(%s)",
                            new Date(),
                            entry.getKey().getAddress().getHostAddress(),
                            entry.getKey().getPort(),
                            entry.getValue().getClass().getName(),
                            entry.getValue().getMessage()
                        ));
                    }).connect();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            };

            // TODO: 汚い
            multicast.setPort(
                port
            ).onReceive(receive -> {
                if (receive.getValue().length == 4) {
                    InetSocketAddress addr = new InetSocketAddress(
                        receive.getKey().getAddress(),
                        ByteBuffer.wrap(receive.getValue()).getInt()
                    );
                    if (!manager.getAllConnections().stream().anyMatch(conn -> conn.addr.equals(addr))) {
                        System.out.println(String.format(
                            "%tT [onBeaconReceive] %s:%d -> %s:%d",
                            new Date(),
                            receive.getKey().getAddress().getHostAddress(),
                            receive.getKey().getPort(),
                            addr.getAddress().getHostAddress(),
                            addr.getPort()
                        ));
                        connect.accept(addr);
                    }
                }
            }).onError((e) -> {
                System.out.println(String.format(
                    "%tT [onError] %s(%s)",
                    new Date(),
                    e.getClass().getName(),
                    e.getMessage()
                ));
            }).start().joinGroup(
                InetAddress.getByName("224.0.0.1")
            ).setKeepAliveData(
                () -> {
                    return ByteBuffer.allocate(4).putInt(manager.getPort()).flip();
                }
            ).connectAsync();

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
