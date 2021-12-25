package org.apache.flink.librariesplus.test;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ceppro.pattern.Pattern;
import org.apache.flink.ceppro.pattern.conditions.RichIterativeCondition;
import org.apache.flink.ceppro.util.CustomStringJavaCompiler;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;

/**
 * @author johnCai
 * @version 1.0
 * @date 2021/12/19 下午9:25
 */
public class JavaCustomStringCompilerTest {
    public static void ___main(String[] args) {
        String a= "public class CustomPatternStringImpl implements GeneratePatternInterface {\\n";
        java.util.regex.Pattern pattern= java.util.regex.Pattern.compile("\\s+class\\s+(\\S+)\\s");
        java.util.regex.Matcher matcher = pattern.matcher(a);
        if (matcher.find()) {
            String str = matcher.group();
            System.out.println(str
                    .replaceFirst("class", "")
                    .replace("\\s+(.*)$", "")
                    .trim());
        }
    }
    public static void main(String[] args) throws Exception {
        String configStr = "package org.apache.flink.librariesplus.test;\n" +
                "\n" +
                "import org.apache.flink.api.java.tuple.Tuple3;\n" +
                "import org.apache.flink.ceppro.GeneratePatternInterface;\n" +
                "import org.apache.flink.ceppro.pattern.Pattern;\n" +
                "import org.apache.flink.ceppro.pattern.conditions.RichIterativeCondition;\n" +
                "import org.apache.flink.streaming.api.windowing.time.Time;\n" +
                "\n" +
                "import java.util.HashMap;\n" +
                "import java.util.Map;\n" +
                "\n" +
                "/**\n" +
                " * @author johnCai\n" +
                " * @version 1.0\n" +
                " * @date 2021/12/19 下午9:46\n" +
                " */\n" +
                "public class CustomPatternStringImpl implements GeneratePatternInterface {\n" +
                "\n" +
                "    public Map<String,Pattern<Tuple3<String, Long, String>,?>> getPatternMap() {\n" +
                "        Pattern<Tuple3<String, Long, String>, ?> pattern = Pattern\n" +
                "                .<Tuple3<String, Long, String>>begin(\"start\")\n" +
                "                .where(new RichIterativeCondition<Tuple3<String, Long, String>>() {\n" +
                "                    @Override\n" +
                "                    public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {\n" +
                "                        return value.f0.equals(\"a\");\n" +
                "                    }\n" +
                "                })\n" +
                "                .followedBy(\"middle\")\n" +
                "                .where(new RichIterativeCondition<Tuple3<String, Long, String>>() {\n" +
                "                    @Override\n" +
                "                    public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {\n" +
                "                        return value.f0.equals(\"b\");\n" +
                "                    }\n" +
                "                }).oneOrMore()\n" +
                "                .within(Time.seconds(5));\n" +
                "        Pattern<Tuple3<String, Long, String>, ?> pattern1 = Pattern\n" +
                "                .<Tuple3<String, Long, String>>begin(\"start\")\n" +
                "                .where(new RichIterativeCondition<Tuple3<String, Long, String>>() {\n" +
                "                    @Override\n" +
                "                    public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {\n" +
                "                        return value.f0.equals(\"a\");\n" +
                "                    }\n" +
                "                })\n" +
                "                .followedBy(\"middle\")\n" +
                "                .where(new RichIterativeCondition<Tuple3<String, Long, String>>() {\n" +
                "                    @Override\n" +
                "                    public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {\n" +
                "                        return value.f0.equals(\"c\");\n" +
                "                    }\n" +
                "                }).or(new RichIterativeCondition<Tuple3<String, Long, String>>() {\n" +
                "                    @Override\n" +
                "                    public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {\n" +
                "                        return value.f0.equals(\"d\");\n" +
                "                    }\n" +
                "                }).oneOrMore()\n" +
                "                .within(Time.seconds(10));\n" +
                "        Map<String, Pattern<Tuple3<String, Long, String>, ?>> patternMap = new HashMap<String, Pattern<Tuple3<String, Long, String>, ?>>(1);\n" +
                "        patternMap.put(\"johnPattern\", pattern);\n" +
                "        patternMap.put(\"johnPattern1\",pattern);\n" +
                "        return patternMap;\n" +
                "    }\n" +
                "}\n";
        CustomStringJavaCompiler<Pattern<String,?>> compiler = new CustomStringJavaCompiler(configStr);
        Map<String, Pattern<String, ?>> stringPatternMap = compiler.runCustomMethod();
        System.out.println(stringPatternMap);
    }
    public static void __main(String[] args) {
        String code = "public class HelloWorld {\n" +
                "    public static void main(String []args) {\n" +
                "\t\tfor(int i=0; i < 5; i++){\n" +
                "\t\t\t       System.out.println(\"Hello World!\");\n" +
                "\t\t}\n" +
                "    }\n" +
                "}";
        CustomStringJavaCompiler compiler = new CustomStringJavaCompiler(code);
        boolean res = compiler.compiler();
        if (res) {
            System.out.println("编译成功");
            System.out.println("compilerTakeTime：" + compiler.getCompilerTakeTime());
            try {
                compiler.runMainMethod();
                System.out.println("runTakeTime：" + compiler.getRunTakeTime());
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println(compiler.getRunResult());
            System.out.println("诊断信息：" + compiler.getCompilerMessage());
        } else {
            System.out.println("编译失败");
            System.out.println(compiler.getCompilerMessage());
        }

    }
}
