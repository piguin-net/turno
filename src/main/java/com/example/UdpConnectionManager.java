package com.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class UdpConnectionManager implements AutoCloseable
{
    private DatagramChannel channel;
    private boolean active = false;
    private int interval = 1_000;
    private Supplier<ByteBuffer> generator = () -> ByteBuffer.allocate(0);
    // private int connectionTimeout = 3 * 60_000;
    private int keepaliveTimeout = 3_000;
    private Map<InetSocketAddress, Long> peers = new HashMap<>();
    private List<Consumer<InetSocketAddress>> connectEventListener = new ArrayList<>();
    private List<Consumer<InetSocketAddress>> keepaliveTimeoutEventListener = new ArrayList<>();
    private List<Consumer<InetSocketAddress>> disconnectEventListener = new ArrayList<>();
    private List<Consumer<Entry<InetSocketAddress, byte[]>>> receiveEventListener = new ArrayList<>();
    private List<Consumer<Entry<InetSocketAddress, Exception>>> errorEventListener = new ArrayList<>();
    private List<Consumer<Exception>> globalErrorEventListener = new ArrayList<>();

    private final Thread receiver = new Thread(() -> {
        while (this.active) {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(65535);
                SocketAddress socket = this.channel.receive(buffer);
                if (socket instanceof InetSocketAddress addr) {
                    byte[] data = new byte[buffer.flip().limit()];
                    buffer.get(data);
                    if (!this.peers.containsKey(addr) || this.peers.get(addr) < 0) {
                        this.dispatchConnectEventListener(addr);
                    }
                    this.peers.put(addr, new Date().getTime());
                    boolean self = Utils.getSiteLocalNetworkInterfaces().values().stream().anyMatch(
                        addrs -> addrs.contains(addr.getAddress().getHostAddress())
                    );
                    if (!self) {
                        this.dispatchReceiveEventListener(addr, data);
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

    private final Thread keepalive = new Thread(() -> {
        while (this.active) {
            try {
                for (InetSocketAddress addr: this.peers.keySet()) {
                    if (this.isKeepAliveTimeout(addr)) {
                        this.peers.put(addr, -1 * new Date().getTime());
                        this.dispatchKeepAliveTimeoutEventListener(addr);
                    }
                    try {
                        this.channel.send(this.generator.get(), addr);
                    } catch (Exception e) {
                        if (this.active) {
                            this.dispatchErrorEventListener(addr, e);
                        }
                    }
                }
                Thread.sleep(this.interval);
            } catch (InterruptedException e) {
                if (this.active) {
                    this.dispatchErrorEventListener(e);
                }
            }
        }
    }, "UdpConnectionManager KeepAliveThread");

    public UdpConnectionManager setKeepAliveInterval(int interval) {
        this.interval = interval;
        return this;
    }
    public UdpConnectionManager setKeepAliveTimeout(int timeout) {
        this.keepaliveTimeout = timeout;
        return this;
    }
    public UdpConnectionManager setKeepAliveData(Supplier<ByteBuffer> generator) {
        this.generator = generator;
        return this;
    }
    public UdpConnectionManager onConnect(Consumer<InetSocketAddress> listener) {
        this.connectEventListener.add(listener);
        return this;
    }
    public UdpConnectionManager onKeepAliveTimeout(Consumer<InetSocketAddress> listener) {
        this.keepaliveTimeoutEventListener.add(listener);
        return this;
    }
    public UdpConnectionManager onDisconnect(Consumer<InetSocketAddress> listener) {
        this.disconnectEventListener.add(listener);
        return this;
    }
    public UdpConnectionManager onReceive(Consumer<Entry<InetSocketAddress, byte[]>> listener) {
        this.receiveEventListener.add(listener);
        return this;
    }
    public UdpConnectionManager onError(Consumer<Entry<InetSocketAddress, Exception>> listener) {
        this.errorEventListener.add(listener);
        return this;
    }
    public UdpConnectionManager onGlobalError(Consumer<Exception> listener) {
        this.globalErrorEventListener.add(listener);
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
    private void dispatchErrorEventListener(Exception e) {
        this.globalErrorEventListener.forEach(
            listener -> new Thread(
                () -> listener.accept(e),
                "UdpConnectionManager ErrorEventListenerThread"
            ).start()
        );
    }

    public UdpConnectionManager() throws IOException {
        this.channel = DatagramChannel.open();
        this.channel.configureBlocking(false);
    }

    public UdpConnectionManager setPort(int port) throws SocketException {
        this.channel.socket().bind(new InetSocketAddress(port));
        return this;
    }

    public Integer getPort() {
        return this.channel.socket().getLocalPort();
    }

    public boolean isActive() {
        return this.active;
    }

    public boolean isKeepAliveTimeout(InetSocketAddress addr) {
        return this.peers.get(addr) > 0 && new Date().getTime() - this.peers.get(addr) > this.keepaliveTimeout;
    }

    public void send(byte[] data, InetSocketAddress addr) throws IOException, InterruptedException {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        this.connect(addr);
        this.channel.send(buffer, addr);
    }

    public void connectAsync(InetSocketAddress addr) {
        if (!this.peers.containsKey(addr)) {
            this.peers.put(addr, -1 * new Date().getTime());
        }
    }

    public void connect(InetSocketAddress addr) throws InterruptedException {
        this.connectAsync(addr);
        while (this.peers.get(addr) < 0) {
            Thread.sleep(1);
        }
    }

    public void disconnect(InetSocketAddress addr) {
        this.peers.remove(addr);
        this.dispatchDisconnectEventListener(addr);
    }

    public UdpConnectionManager joinGroup(InetAddress group) throws IOException {
        for (NetworkInterface nic: Utils.getSiteLocalNetworkInterfaces().keySet()) {
            this.channel.join(group, nic);
        }
        // TODO: 汚い
        this.peers.put(new InetSocketAddress(group, this.getPort()), Long.MAX_VALUE);
        return this;
    }

    public List<InetSocketAddress> getAllConnections() {
        return new ArrayList<>(this.peers.keySet());
    }

    public List<InetSocketAddress> getActiveConnections() {
        return this.peers.keySet().stream().filter(
            addr -> !this.isKeepAliveTimeout(addr)
        ).toList();
    }

    public void start() {
        this.active = true;
        this.receiver.start();
        this.keepalive.start();
    }

    @Override
    public void close() throws InterruptedException, IOException {
        this.active = false;
        this.receiver.join();
        this.keepalive.join();
        this.channel.close();
    }

    public static void main( String[] args ) throws InterruptedException, IOException
    {
        Integer port = Integer.getInteger("chat.port", 9625);
        String group = System.getProperty("chat.group", "224.0.0.1");

        try (
            UdpConnectionManager manager = new UdpConnectionManager();
            UdpConnectionManager multicast = new UdpConnectionManager();
        ) {
            manager.setKeepAliveData(
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
            }).onGlobalError(e -> {
                System.out.println(String.format(
                    "%tT [onError] %s(%s)",
                    new Date(),
                    e.getClass().getName(),
                    e.getMessage()
                ));
            }).start();

            multicast.setPort(
                port
            ).joinGroup(
                InetAddress.getByName(group)
            ).setKeepAliveData(
                () -> {
                    return ByteBuffer.allocate(2).putShort(manager.getPort().shortValue()).flip();
                }
            ).onReceive(receive -> {
                if (receive.getValue().length == 2) {
                    InetSocketAddress addr = new InetSocketAddress(
                        receive.getKey().getAddress(),
                        Utils.ushort2int(ByteBuffer.wrap(receive.getValue()).getShort())
                    );
                    if (!manager.getAllConnections().contains(addr)) {
                        System.out.println(String.format(
                            "%tT [onBeaconReceive] %s:%d -> %s:%d",
                            new Date(),
                            receive.getKey().getAddress().getHostAddress(),
                            receive.getKey().getPort(),
                            addr.getAddress().getHostAddress(),
                            addr.getPort()
                        ));
                        manager.connectAsync(addr);
                    }
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
            }).onGlobalError(e -> {
                System.out.println(String.format(
                    "%tT [onError] %s(%s)",
                    new Date(),
                    e.getClass().getName(),
                    e.getMessage()
                ));
            }).start();

            try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in));) {
                while (true) {
                    String message = input.readLine();
                    if (message == null || "".equals(message.trim())) break;
                    for (InetSocketAddress addr: manager.getActiveConnections()) {
                        manager.send(message.getBytes(Charset.defaultCharset()), addr);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
