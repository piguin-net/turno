package com.example;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.FutureTask;

public class SimpleStun {
    public static enum MessageType {
        BINDING_REQUEST((short)0x0001),
        BINDING_SUCCESS_RESPONSE((short)0x0101);
        private final short value;
        private MessageType(short value) {
            this.value = value;
        }
        public short value() {
            return this.value;
        }
    }

    public static enum AttributeType {
        MAPPED_ADDRESS((short)0x0001, false),
        XOR_MAPPED_ADDRESS((short)0x0020, true);
        private final short value;
        private final boolean mask;
        private AttributeType(short value, boolean mask) {
            this.value = value;
            this.mask = mask;
        }
        public static AttributeType of(short value) {
            for (AttributeType item: AttributeType.values()) {
                if (item.value() == value) return item;
            }
            return null;
        }
        public short value() {
            return this.value;
        }
        public boolean mask() {
            return this.mask;
        }
    }

    public static enum MappedAddressFamily {
        IPv4((short)0x0001, (short)(32 / 8)),
        IPv6((short)0x0002, (short)(128 / 8));
        private final short value;
        private final short length;
        private MappedAddressFamily(short value, short length) {
            this.value = value;
            this.length = length;
        }
        public static MappedAddressFamily of(short value) {
            for (MappedAddressFamily item: MappedAddressFamily.values()) {
                if (item.value() == value) return item;
            }
            return null;
        }
        public short value() {
            return this.value;
        }
        public short length() {
            return this.length;
        }
    }

    private static final int COOKIE = 0x2112A442;

    /**
     *  0                   1                   2                   3
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |0 0|   Message Type (0x0001)   |      Message Length (0)       |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |                   Magic Cookie (0x2112A442)                   |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |                                                               |
     * |                Transaction ID (Random 96 bits)                |
     * |                                                               |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     */
    public static ByteBuffer generateRequest() {
        return generateRequest(
            COOKIE,
            new Random().nextInt(),
            new Random().nextInt(),
            new Random().nextInt()
        );
    }

    public static ByteBuffer generateRequest(int cookie) {
        return generateRequest(
            cookie,
            new Random().nextInt(),
            new Random().nextInt(),
            new Random().nextInt()
        );
    }

    public static ByteBuffer generateRequest(int transactionId1, int transactionId2, int transactionId3) {
        return generateRequest(
            COOKIE,
            transactionId1,
            transactionId2,
            transactionId3
        );
    }

    public static ByteBuffer generateRequest(int cookie, int transactionId1, int transactionId2, int transactionId3) {
        ByteBuffer request = ByteBuffer.allocate(20);
        request.putShort(MessageType.BINDING_REQUEST.value());
        request.putShort((short) 0);
        request.putInt(cookie);
        request.putInt(transactionId1);
        request.putInt(transactionId2);
        request.putInt(transactionId3);
        return request.flip();
    }

    /**
     *  0                   1                   2                   3
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |0 0|   Message Type (0x0101)   |         Message Length        |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |                   Magic Cookie (0x2112A442)                   |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |                                                               |
     * |                Transaction ID (Random 96 bits)                |
     * |                                                               |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |        Attribute  Type        |       Attribute  Length       |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |                         Value (variable)                      |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * 
     * ....
     * 
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |    Attribute Type (0x0020)    |       Attribute  Length       |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |   Family (0x0001 or 0x0002)   |           Port                |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |                                                               |
     * |           Address (IPv4[32 bits] or IPv6[128 bits])           |
     * |                                                               |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * 
     * port = port ^ (Magic Cookie >> 16)
     * ipv4 = addr ^ (Magic Cookie)
     * ipv6 = addr ^ (Magic Cookie + Transaction ID)
     * 
     * @throws UnknownHostException 
     */
    public static InetSocketAddress parseResponse(ByteBuffer response) throws UnknownHostException {
        short messageType = response.getShort();
        int messageLength = ushort2int(response.getShort());
        if (messageType == MessageType.BINDING_SUCCESS_RESPONSE.value()) {
            int cookie = response.getInt();
            byte[] tran = new byte[96 / 8];
            response.get(tran);
            int limit = response.position() + messageLength;
            while (response.position() < limit) {
                AttributeType attributeType = AttributeType.of(response.getShort());
                int attributeLength = ushort2int(response.getShort());
                if (attributeType != null) {
                    MappedAddressFamily family = MappedAddressFamily.of(response.getShort());
                    int port = ushort2int(response.getShort());
                    if (family != null) {
                        byte[] addr = new byte[family.length()];
                        response.get(addr);
                        if (attributeType.mask()) {
                            port = port ^ (cookie >> 16);
                            byte[] mask = ByteBuffer.allocate(128 / 8).putInt(cookie).put(tran).array();
                            for (int i = 0; i < addr.length; i++) addr[i] ^= mask[i];
                        }
                        return new InetSocketAddress(InetAddress.getByAddress(addr), port);
                    } else {
                        response.position(
                            response.position() + (attributeLength - 4) // family(2byte), port(2byte)
                        );
                    }
                } else {
                    response.position(response.position() + attributeLength);
                }
            }
        }
        return null;
    }

    private static int ushort2int(short value) {
        return (value < 0 ? 1 + Short.MAX_VALUE : 0) + (value & Short.MAX_VALUE);
    }

    private static void printBytes(byte[] data) {
        int i = 0;
        while (i < data.length) {
            int j = 0;
            while (i < data.length && j < 4) {
                System.out.print(String.format(" %02x", data[i]));
                i++;
                j++;
            }
            System.out.println();
        }
    }

    public static void main(String[] args) throws IOException {
        InetSocketAddress server = new InetSocketAddress(
            System.getProperty("stun.server.addr", "stun.l.google.com"),
            Integer.getInteger("stun.server.port", 19302)
        );

        DatagramChannel channel = DatagramChannel.open();

        for (InetAddress addr: InetAddress.getAllByName(server.getHostName())) {
            try {
                ByteBuffer request = SimpleStun.generateRequest();
                InetSocketAddress target = new InetSocketAddress(addr, server.getPort());

                FutureTask<byte[]> task = new FutureTask<>(() -> {
                    while (true) {
                        ByteBuffer buf = ByteBuffer.allocate(65535);
                        if (channel.receive(buf) instanceof InetSocketAddress sender) {
                            if (sender.equals(target)) {
                                byte[] data = new byte[buf.flip().limit()];
                                buf.get(data);
                                return data;
                            }
                        }
                        Thread.sleep(1);
                    }
                });
                new Thread(task).start();

                channel.send(request, target);
                byte[] data = new byte[request.limit()];
                request.position(0).get(data);
                System.out.println(String.format(
                    "send stun to %s:%d",
                    addr.getHostAddress(),
                    server.getPort()
                ));
                printBytes(data);

                byte[] response = task.get();
                System.out.println(String.format(
                    "receive stun from %s:%d",
                    target.getAddress().getHostAddress(),
                    target.getPort()
                ));
                printBytes(response);

                InetSocketAddress mapped = SimpleStun.parseResponse(ByteBuffer.wrap(response));
                if (mapped != null) {
                    System.out.println(String.format(
                        "mapped addr=%s, port=%d",
                        mapped.getAddress().getHostAddress(),
                        mapped.getPort()
                    ));
                } else {
                    System.out.println("error.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println(String.join("", Collections.nCopies(80, "=")));
        }
    }
}
