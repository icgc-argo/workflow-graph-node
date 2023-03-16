package org.icgc_argo.workflowgraphnode.rabbitmq;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.icgc_argo.workflow_graph_lib.polyglot.GuestLangGraphExceptionUtils;
import org.icgc_argo.workflow_graph_lib.polyglot.NestedProxyObject;
import org.icgc_argo.workflow_graph_lib.polyglot.enums.GraphFunctionLanguage;
import org.icgc_argo.workflow_graph_lib.polyglot.exceptions.GraphFunctionException;
import org.icgc_argo.workflow_graph_lib.polyglot.exceptions.GraphFunctionUnsupportedLanguageException;
import org.icgc_argo.workflow_graph_lib.polyglot.exceptions.GraphFunctionValueException;
import org.icgc_argo.workflow_graph_lib.utils.JacksonUtils;
import org.icgc_argo.workflow_graph_lib.utils.PatternMatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PolyglotCustom {

  private static final Logger log = LoggerFactory.getLogger(PolyglotCustom.class);
  protected static final Context ctx = buildPolyglotCtx();

  public PolyglotCustom() {
  }

  public static Map<String, Object> runMainFunctionWithData(GraphFunctionLanguage language, String scriptContent, Map<String, Object> data) {
    try {
      Map<String, Object> returnedValue = (Map)((Value) PatternMatch.match(language).on((lang) -> {
        return lang.equals(GraphFunctionLanguage.JS);
      }, () -> {
        return runJsScript(String.format("function main(data) { %s }", scriptContent), data);
      }).on((lang) -> {
        return lang.equals(GraphFunctionLanguage.PYTHON);
      }, () -> {
        return runPythonScript(String.format("def main(data):\n    %s", scriptContent), data);
      }).otherwise(() -> {
        throw new GraphFunctionUnsupportedLanguageException(String.format("Operation %s is not supported", language));
      })).as(Map.class);
      GuestLangGraphExceptionUtils.throwErrorIfMapIsGuestLangGraphException(returnedValue);
      return returnedValue;
    } catch (PolyglotException var4) {
      throw new GraphFunctionException(var4.getLocalizedMessage());
    } catch (ClassCastException | IllegalStateException var5) {
      throw new GraphFunctionValueException(String.format("Unable to convert returned value to Map<String, Object>: %s", var5.getLocalizedMessage()));
    }
  }

  public static Map<String, Object> runMainFunctionWithData(GraphFunctionLanguage language, String scriptContent, String data) {
    return runMainFunctionWithData(language, scriptContent, JacksonUtils.toMap(data));
  }

  public static Boolean evaluateBooleanExpression(GraphFunctionLanguage language, String expression, Map<String, Object> data) {
    try {
      Value returnValue = (Value)PatternMatch.match(language).on((lang) -> {
        return lang.equals(GraphFunctionLanguage.JS);
      }, () -> {
        return runJsScript(String.format("function main(data) { return %s; }", expression), data);
      }).on((lang) -> {
        return lang.equals(GraphFunctionLanguage.PYTHON);
      }, () -> {
        return runPythonScript(String.format("def main(data):\n    return %s", expression), data);
      }).otherwise(() -> {
        throw new GraphFunctionValueException(String.format("Operation %s is not supported", language));
      });
      if (!returnValue.isBoolean()) {
        throw new GraphFunctionValueException("Return value must be of boolean type");
      } else {
        return returnValue.asBoolean();
      }
    } catch (PolyglotException var4) {
      throw new GraphFunctionException(var4.getLocalizedMessage());
    }
  }

  public static Boolean evaluateBooleanExpression(GraphFunctionLanguage language, String expression, String data) {
    return evaluateBooleanExpression(language, expression, JacksonUtils.toMap(data));
  }

  protected static Value runJsScript(String jsScript, Map<String, Object> eventMap) {
    return runFunctionMain("js", "js", "script.js", jsScript, eventMap);
  }

  protected static Value runPythonScript(String pythonScript, Map<String, Object> eventMap) {
    return runFunctionMain("python", "python", "script.py", pythonScript, eventMap);
  }

  protected synchronized static Value runFunctionMain(String language, String languageId, String scriptFileName, String script, Map<String, Object> data) {
    NestedProxyObject eventMapProxy = new NestedProxyObject(data);
    Source source = Source.newBuilder(language, script, scriptFileName).buildLiteral();
    ctx.eval(source);
    return ctx.getBindings(languageId).getMember("main").execute(new Object[]{eventMapProxy});
  }

  private synchronized static Context buildPolyglotCtx() {
    Context ctx = Context.newBuilder(new String[]{"python", "js"}).build();

    try {
      ctx.eval(GuestLangGraphExceptionUtils.buildJsGraphExceptionCreator("reject", GuestLangGraphExceptionUtils.GraphExceptionTypes.CommittableException));
      ctx.eval(GuestLangGraphExceptionUtils.buildJsGraphExceptionCreator("requeue", GuestLangGraphExceptionUtils.GraphExceptionTypes.RequeueableException));
      ctx.eval(GuestLangGraphExceptionUtils.buildPythonGraphExceptionCreator("reject", GuestLangGraphExceptionUtils.GraphExceptionTypes.CommittableException));
      ctx.eval(GuestLangGraphExceptionUtils.buildPythonGraphExceptionCreator("requeue", GuestLangGraphExceptionUtils.GraphExceptionTypes.RequeueableException));
    } catch (Exception var2) {
      log.error("Failed to add exception object creators to polyglot context!");
    }

    return ctx;
  }
}
