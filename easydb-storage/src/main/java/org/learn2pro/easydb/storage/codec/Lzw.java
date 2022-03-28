package org.learn2pro.easydb.storage.codec;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

public class Lzw {

    public static List<Integer> encode(String input) {
        System.out.println("Encoding...");
        Map<String, Integer> table = Maps.newHashMap();
        for (int i = 0; i < 256; i++) {
            table.put("" + (char) i, i);
        }
        String p = "", c = "";
        p += input.substring(0, 1);
        int code = 256;
        List<Integer> output = Lists.newArrayList();
        for (int i = 0; i < input.length(); i++) {
            if (i != input.length() - 1) {
                c += input.substring(i + 1, i + 2);
            }
            if (table.containsKey(p + c)) {
                p = p + c;
            } else {
                System.out.println(String.format("%s\t%s\t\t%s\t%s", p, table.get(p), p + c, code));
                output.add(table.get(p));
                table.put(p + c, code);
                code++;
                p = c;
            }
            c = "";
        }
        output.add(table.get(p));
        return output;
    }

    public static String decode(List<Integer> input) {
        System.out.println("Decoding...");
        Map<Integer, String> table = Maps.newHashMap();
        for (int i = 0; i < 256; i++) {
            table.put(i, "" + (char) i);
        }
        Integer old = input.get(0), n;
        String s = table.get(old), c = "";
        c += s.substring(0, 1);
        System.out.println("s:" + s);
        int count = 256;
        StringBuilder sb = new StringBuilder(s);
        for (int i = 0; i < input.size() - 1; i++) {
            n = input.get(i + 1);
            String pre = s;
            if (!table.containsKey(n)) {
                s = table.get(old);
                s = s + c;
            } else {
                s = table.get(n);
            }
            System.out.printf("pre:%s,s:%s,c:%s,old:%s,n:%s,count:%s\n", pre, s, c, old,n, count);
            sb.append(s);
            c = "";
            c += s.substring(0, 1);
            table.put(count, table.get(old) + c);
            count++;
            old = n;
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        String s = "BABAABAAA";
        List<Integer> output = encode(s);
        System.out.println(output);
        String answer = decode(output);
        System.out.println(answer);
        assert answer.equals(s);
    }

}
