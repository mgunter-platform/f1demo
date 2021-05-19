package com.mgunter.provided;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.XORShiftRandom;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Categories are taken from: https://support.google.com/merchants/answer/6324436?hl=en
 */
public class ClickEventGenerator extends RichParallelSourceFunction<String> {
    private final long numberOfUsers;

    private boolean running = true;
    private ObjectMapper mapper;
    private long eventTimeMillis;
    private static Random RND = new XORShiftRandom(1337);
    private List<String> categories;
    private List<User> users;

    public ClickEventGenerator(ParameterTool pt) {
        this.numberOfUsers = pt.getLong("generator.numberOfUsers", 10_000_000L);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.mapper = new ObjectMapper();
        // lets start with the real time
        this.eventTimeMillis = System.currentTimeMillis();
        this.categories = readCategories();
        this.users = new ArrayList<>();
        // create initial users
        for(int i = 0; i < 50_000; i++) {
            users.add(new AuthenticatedUser(i, getNextIP()));
        }
    }

    private List<String> readCategories() {
        InputStream in = ClickEventGenerator.class.getClassLoader().getResourceAsStream("categories.txt");
        ArrayList<String> out = new ArrayList<>();
        Scanner s = new Scanner(in);
        s.useDelimiter("\n");
        while(s.hasNext()) {
            out.add(s.next());
        }
        s.close();
        return out;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        long sequenceId = 0;
        while(running) {
           // Thread.sleep(10);
            ObjectNode node = mapper.createObjectNode();

            User u = getNextUser();
            long ts = getNextTimestamp();

            node.put("user.ip", intIPtoString(u.getIP()));
            node.put("user.accountId", u.getId());

            node.put("page", getNextUrl());

            node.put("host.timestamp", (new java.util.Date(ts)).toLocaleString() );
            // write fake hostname for globalcorp.com's berlin load balancer
            node.put("host", String.format("lb-brl-%d-%d.globalcorp.com",
                    getRuntimeContext().getNumberOfParallelSubtasks(),
                    getRuntimeContext().getIndexOfThisSubtask())
                );
            node.put("host.sequence", sequenceId);
           // System.out.println(mapper.writeValueAsString(node));

            sourceContext.collectWithTimestamp(mapper.writeValueAsString(node), ts);
            sequenceId++;

            if (sequenceId % 10000 == 0) {
                sourceContext.emitWatermark(new Watermark(ts - 60000)); // 1 min max delay
                System.out.println("Watermark emmitted at ts=" + (ts -60000));

            }
        }
    }

    private static String intIPtoString(int ip) {
        int b1 = (ip >>> 24) & 0xff;
        int b2 = (ip >>> 16) & 0xff;
        int b3 = (ip >>> 8) & 0xff;
        int b4 = ip  & 0xff;
        return new StringBuffer().append(b1).append(".")
                .append(b2).append(".")
                .append(b3).append(".")
                .append(b4)
                .toString();
    }

    private static int getNextIP() {
        return RND.nextInt();
    }

    private final static User anonymousUser = new User() {
        @Override
        public long getId() {
            return -1;
        }

        @Override
        public int getIP() {
            return getNextIP();
        }
    };

    private User getNextUser() {
        if(RND.nextBoolean()) {
            return anonymousUser;
        }
        // replace one user (note: this might generate data with the same user being logged in multiple times)
        users.set(RND.nextInt(users.size()), new AuthenticatedUser(ThreadLocalRandom.current().nextLong(this.numberOfUsers), getNextIP()));

        // get a user from the local state.
        int userIndex = RND.nextInt(users.size());
        return users.get(userIndex);
    }

    private String getNextUrl() {
        StringBuffer sb = new StringBuffer();
        int id = RND.nextInt(3_000_000);
        switch(id % 7) {
            case 0:
            case 1:
                sb.append("/categories/");
                sb.append(categories.get(id % categories.size()));
                break;
            case 2:
            case 3:
            case 4:
                sb.append("/productDetails/");
                sb.append(id / 10); // strip last digit
                break;
            case 5:
                sb.append("/blog/");
                sb.append(id / 10);
                break;
            case 6:
                sb.append("/search?q=");
                sb.append(RandomStringUtils.randomAlphabetic(id % 5));
                break;
            default:
                throw new RuntimeException("that's kinda unexpected right now");

        }

        return sb.toString();
    }

    private long getNextTimestamp() {
        this.eventTimeMillis += 10;
        return (this.eventTimeMillis) - RND.nextInt(60 * 1000); // max delay of one minute
    }

    @Override
    public void cancel() {
        running = false;
    }

    public static void main(String[] args) {
    }

    private interface User {
        long getId();

        int getIP();
    }
    private static class AuthenticatedUser implements User {

        private long id;
        private int ip;

        public AuthenticatedUser(long id, int ip) {
            this.id = id;
            this.ip = ip;
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public int getIP() {
            return ip;
        }
    }
}
