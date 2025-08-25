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
    private Map<InetSocketAddress, Integer> intervals = new HashMap<>();
    private Supplier<ByteBuffer> generator = () -> ByteBuffer.allocate(0);
    private Map<InetSocketAddress, Supplier<ByteBuffer>> generators = new HashMap<>();
    // private int connectionTimeout = 3 * 60_000;
    private int keepaliveTimeout = 3_000;
    private Map<InetSocketAddress, Long> peers = new HashMap<>();
    private List<Consumer<InetSocketAddress>> connectEventListener = new ArrayList<>();
    private List<Consumer<InetSocketAddress>> keepaliveTimeoutEventListener = new ArrayList<>();
    private List<Consumer<InetSocketAddress>> disconnectEventListener = new ArrayList<>();
    private List<Consumer<Entry<InetSocketAddress, byte[]>>> receiveEventListener = new ArrayList<>();
    private List<Consumer<Entry<InetSocketAddress, Exception>>> errorEventListener = new ArrayList<>();
    private Map<InetSocketAddress, Runnable> connectEventListeners = new HashMap<>();
    private Map<InetSocketAddress, Runnable> keepaliveTimeoutEventListeners = new HashMap<>();
    private Map<InetSocketAddress, Runnable> disconnectEventListeners = new HashMap<>();
    private Map<InetSocketAddress, Consumer<byte[]>> receiveEventListeners = new HashMap<>();
    private Map<InetSocketAddress, Consumer<Exception>> errorEventListeners = new HashMap<>();
    private List<Consumer<Exception>> globalErrorEventListener = new ArrayList<>();
    private Map<InetSocketAddress, Thread> keepalive = new HashMap<>();

    private final Thread receiver = new Thread(() -> {
        while (this.active) {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(65535);
                SocketAddress socket = this.channel.receive(buffer);
                if (socket instanceof InetSocketAddress addr) {
                    byte[] data = new byte[buffer.flip().limit()];
                    buffer.get(data);
                    boolean isUnknown = !this.peers.containsKey(addr);
                    boolean isFirst = isUnknown || this.peers.get(addr) < 0;
                    this.peers.put(addr, new Date().getTime());
                    if (isUnknown) {
                        this.startKeepalive(addr);
                    }
                    if (isFirst) {
                        this.dispatchConnectEventListener(addr);
                    }
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
    });

    public UdpConnectionManager setKeepAliveInterval(int interval) {
        this.interval = interval;
        return this;
    }
    public UdpConnectionManager setKeepAliveInterval(InetSocketAddress addr, int interval) {
        this.intervals.put(addr, interval);
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
    public UdpConnectionManager setKeepAliveData(InetSocketAddress addr, Supplier<ByteBuffer> generator) {
        this.generators.put(addr, generator);
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
    public UdpConnectionManager onConnect(InetSocketAddress addr, Runnable listener) {
        this.connectEventListeners.put(addr, listener);
        return this;
    }
    public UdpConnectionManager onKeepAliveTimeout(InetSocketAddress addr, Runnable listener) {
        this.keepaliveTimeoutEventListeners.put(addr, listener);
        return this;
    }
    public UdpConnectionManager onDisconnect(InetSocketAddress addr, Runnable listener) {
        this.disconnectEventListeners.put(addr, listener);
        return this;
    }
    public UdpConnectionManager onReceive(InetSocketAddress addr, Consumer<byte[]> listener) {
        this.receiveEventListeners.put(addr, listener);
        return this;
    }
    public UdpConnectionManager onError(InetSocketAddress addr, Consumer<Exception> listener) {
        this.errorEventListeners.put(addr, listener);
        return this;
    }
    public UdpConnectionManager onGlobalError(Consumer<Exception> listener) {
        this.globalErrorEventListener.add(listener);
        return this;
    }
    private void dispatchConnectEventListener(InetSocketAddress addr) {
        String name = String.format(
            "UdpConnectionManager ConnectEventListenerThread(%s:%d)",
            addr.getAddress().getHostAddress(),
            addr.getPort()
        );
        this.connectEventListener.forEach(
            listener -> new Thread(() -> listener.accept(addr), name).start()
        );
        if (this.connectEventListeners.containsKey(addr)) {
            new Thread(() -> this.connectEventListeners.get(addr).run(), name).start();
        }
    }
    private void dispatchKeepAliveTimeoutEventListener(InetSocketAddress addr) {
        String name = String.format(
            "UdpConnectionManager KeepaliveTimeoutEventListenerThread(%s:%d)",
            addr.getAddress().getHostAddress(),
            addr.getPort()
        );
        this.keepaliveTimeoutEventListener.forEach(
            listener -> new Thread(() -> listener.accept(addr), name).start()
        );
        if (this.keepaliveTimeoutEventListeners.containsKey(addr)) {
            new Thread(() -> this.keepaliveTimeoutEventListeners.get(addr).run(), name).start();
        }
    }
    private void dispatchDisconnectEventListener(InetSocketAddress addr) {
        String name = String.format(
            "UdpConnectionManager DisconnectEventListenerThread(%s:%d)",
            addr.getAddress().getHostAddress(),
            addr.getPort()
        );
        this.disconnectEventListener.forEach(
            listener -> new Thread(() -> listener.accept(addr), name).start()
        );
        if (this.disconnectEventListeners.containsKey(addr)) {
            new Thread(() -> this.disconnectEventListeners.get(addr).run(), name).start();
        }
    }
    private void dispatchReceiveEventListener(InetSocketAddress addr, byte[] data) {
        String name = String.format(
            "UdpConnectionManager ReceiveEventListenerThread(%s:%d)",
            addr.getAddress().getHostAddress(),
            addr.getPort()
        );
        this.receiveEventListener.forEach(
            listener -> new Thread(() -> listener.accept(Map.entry(addr, data)), name).start()
        );
        if (this.receiveEventListeners.containsKey(addr)) {
            new Thread(() -> this.receiveEventListeners.get(addr).accept(data), name).start();
        }
    }
    private void dispatchErrorEventListener(InetSocketAddress addr, Exception e) {
        String name = String.format(
            "UdpConnectionManager ErrorEventListenerThread(%s:%d)",
            addr.getAddress().getHostAddress(),
            addr.getPort()
        );
        this.errorEventListener.forEach(
            listener -> new Thread(() -> listener.accept(Map.entry(addr, e)), name).start()
        );
        if (this.errorEventListeners.containsKey(addr)) {
            new Thread(() -> this.errorEventListeners.get(addr).accept(e), name).start();
        }
    }
    private void dispatchErrorEventListener(Exception e) {
        String name = "UdpConnectionManager ErrorEventListenerThread";
        this.globalErrorEventListener.forEach(
            listener -> new Thread(() -> listener.accept(e), name).start()
        );
    }

    public UdpConnectionManager() throws IOException {
        this.channel = DatagramChannel.open();
        this.channel.configureBlocking(false);
        this.receiver.setName(String.format("UdpConnectionManager ReceiverThread(:%d)", this.getPort()));
    }

    public UdpConnectionManager setPort(int port) throws SocketException {
        this.channel.socket().bind(new InetSocketAddress(port));
        this.receiver.setName(String.format("UdpConnectionManager ReceiverThread(:%d)", this.getPort()));
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

    private void startKeepalive(InetSocketAddress addr) {
        String name = String.format(
            "UdpConnectionManager KeepAliveThread(%s:%d)",
            addr.getAddress().getHostAddress(),
            addr.getPort()
        );
        Thread thread = new Thread(() -> {
            while (this.keepalive.containsKey(addr)) {
                try {
                    if (this.isKeepAliveTimeout(addr)) {
                        this.peers.put(addr, -1 * new Date().getTime());
                        this.dispatchKeepAliveTimeoutEventListener(addr);
                    }
                    try {
                        if (this.generators.containsKey(addr)) {
                            this.channel.send(this.generators.get(addr).get(), addr);
                        } else {
                            this.channel.send(this.generator.get(), addr);
                        }
                    } catch (Exception e) {
                        if (this.active) {
                            this.dispatchErrorEventListener(addr, e);
                        }
                    }
                    if (this.intervals.containsKey(addr)) {
                        Thread.sleep(this.intervals.get(addr));
                    } else {
                        Thread.sleep(this.interval);
                    }
                } catch (Exception e) {
                    if (this.active) {
                        this.dispatchErrorEventListener(e);
                    }
                }
            }
        }, name);
        this.keepalive.put(addr, thread);
        if (this.active) thread.start();
    }

    public void connectAsync(InetSocketAddress addr) {
        if (!this.peers.containsKey(addr)) {
            this.peers.put(addr, -1 * new Date().getTime());
            this.startKeepalive(addr);
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
        this.intervals.remove(addr);
        this.generators.remove(addr);
        this.connectEventListeners.remove(addr);
        this.keepaliveTimeoutEventListeners.remove(addr);
        this.disconnectEventListeners.remove(addr);
        this.receiveEventListeners.remove(addr);
        this.errorEventListeners.remove(addr);
        this.keepalive.remove(addr);
        this.dispatchDisconnectEventListener(addr);
    }

    public UdpConnectionManager joinGroup(InetAddress group) throws IOException {
        for (NetworkInterface nic: Utils.getSiteLocalNetworkInterfaces().keySet()) {
            this.channel.join(group, nic);
        }
        InetSocketAddress addr = new InetSocketAddress(group, this.getPort());
        this.peers.put(addr, Long.MAX_VALUE); // TODO: 汚い
        this.startKeepalive(addr);
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
        this.keepalive.values().forEach(thread -> {
            if (!thread.isAlive()) {
                thread.start();
            }
        });
    }

    @Override
    public void close() throws InterruptedException, IOException {
        this.active = false;
        this.receiver.join();
        List<Thread> threads = new ArrayList<>(this.keepalive.values());
        this.keepalive.clear();
        for (Thread thread: threads) thread.join();
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
                System.err.println(String.format(
                    "%tT [onError] %s:%d",
                    new Date(),
                    entry.getKey().getAddress().getHostAddress(),
                    entry.getKey().getPort()
                ));
                entry.getValue().printStackTrace();
            }).onGlobalError(e -> {
                System.err.println(String.format(
                    "%tT [onError]",
                    new Date()
                ));
                e.printStackTrace();
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
                System.err.println(String.format(
                    "%tT [onError] %s:%d",
                    new Date(),
                    entry.getKey().getAddress().getHostAddress(),
                    entry.getKey().getPort()
                ));
                entry.getValue().printStackTrace();
            }).onGlobalError(e -> {
                System.err.println(String.format(
                    "%tT [onError]",
                    new Date()
                ));
                e.printStackTrace();
            }).start();

            try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in));) {
                while (true) {
                    String message = input.readLine();
                    if (message == null || "".equals(message.trim())) break;
                    for (InetSocketAddress addr: manager.getActiveConnections()) {
                        // TODO: groupは分けて管理
                        if (!group.equals(addr.getAddress().getHostAddress())) {
                            manager.send(message.getBytes(Charset.defaultCharset()), addr);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
