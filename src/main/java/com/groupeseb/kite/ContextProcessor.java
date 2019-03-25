package com.groupeseb.kite;

import com.google.common.base.Charsets;
import com.groupeseb.kite.check.Check;
import com.groupeseb.kite.function.Function;
import com.groupeseb.kite.function.impl.UUIDFunction;
import com.jayway.restassured.specification.RequestSpecification;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.assertj.core.util.Strings;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import javax.annotation.Nullable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.jayway.restassured.RestAssured.given;
import static java.util.Objects.requireNonNull;

/**
 * Bean having the responsibility to replace any placeholders & functions calls using {@link KiteContext}
 * as data provider.
 * <p>
 * Look at {@link ContextProcessor#AUTOMATIC_CHECK_AUTHORIZATION_HEADER_VALUE} for more details about the authorization-aware automaticCheck
 */
@Data
@Slf4j
public class ContextProcessor {
	private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("\\{\\{Timestamp:Now\\}\\}",
			Pattern.CASE_INSENSITIVE);
	private static final Pattern UUID_PATTERN = Pattern.compile("\\{\\{" + UUIDFunction.NAME + ":(.+?)\\}\\}");

	/**
	 * This is the variable name expected to be found in "variables" section of the kiteContext which will be used during authentication on automaticCheck.
	 * See {@link KiteContext#getAuthorizationHeaderNameForAutomaticCheck()}
	 */
	private static final String AUTOMATIC_CHECK_AUTHORIZATION_HEADER_VALUE = "internalCheckFullyAuthenticated";
	private final KiteContext kiteContext;
	private final Collection<Function> availableFunctions;

	private String processedBody;

	ContextProcessor(Collection<Function> availableFunctions,
					 KiteContext kiteContext) {
		this.availableFunctions = availableFunctions;
		this.kiteContext = kiteContext;
	}

	/**
	 * Applies function with given name on given value with function
	 * placeholders
	 *
	 * @param function                 current function
	 * @param valueWithPlaceHolders    value on which function is applied
	 * @param jsonEscapeFunctionResult true if function result must be json-escaped prior replacing
	 *                                 placeholder in valueWithPlaceholder
	 * @return the copy of initial valueWithPlaceholders with function's
	 * placeholders replaced
	 */
	private String executeFunctions(Function function, String valueWithPlaceHolders, boolean jsonEscapeFunctionResult) {

		Matcher matcher = function.getPattern().matcher(valueWithPlaceHolders);

		if (function.isWithParameters()) {
			String result = valueWithPlaceHolders;
			while (matcher.find()) {
				List<String> parameters = new ArrayList<>();

				for (int i = 1; i <= matcher.groupCount(); ++i) {
					// If function parameter contains JSON special character,
					// they may be encoded by the JSON parser (if value with
					// placeholder is a JSON String).
					// It is necessary to unescape them before using
					// them in the function
					parameters.add(StringEscapeUtils.unescapeJson(matcher.group(i)));
				}

				String functionResult = function.apply(parameters, this);
				if (jsonEscapeFunctionResult) {
					functionResult = JSONObject.escape(functionResult);
				}

				result = result.replace(matcher.group(0), functionResult);
			}
			return result;
		}

		return matcher.replaceAll(function.apply());
	}

	/**
	 * Apply all available functions for this creation log to replace
	 * placeholders in given value
	 *
	 * @param valueWithPlaceholders    the String containing the placeholders to replace. This String
	 *                                 may be (or not) the representation of a JSON object (with
	 *                                 escaped values in keys and String values). Thus, placeholders
	 *                                 are json-unescaped before being processed (this have no effect
	 *                                 if this parameter is not a Json String)
	 * @param jsonEscapeFunctionResult if true, function result will be json-escaped before being
	 *                                 replaced in processed string. If false, function return is
	 *                                 injected as is.
	 *                                 <p/>
	 *                                 <b>Use true if the result must be directly parsed as JSON</b>,
	 *                                 false otherwise.
	 * @return the copy of initial valueWithPlaceholders with function's
	 * placeholders replaced
	 */
	String applyFunctions(String valueWithPlaceholders, boolean jsonEscapeFunctionResult) {
		String result = valueWithPlaceholders;

		for (Function availableAbstractFunction : availableFunctions) {
			result = executeFunctions(availableAbstractFunction, result, jsonEscapeFunctionResult);
		}

		Matcher timestampMatcher = TIMESTAMP_PATTERN.matcher(result);
		if (!timestampMatcher.find()) {
			return result;
		}

		// 'Timestamp' is not implemented like other functions, because that
		// would not permit to
		// generate the same date for the whole command (since function is
		// called for each
		// placeholder and not one time by)
		String currentDateString = DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(new Date());
		if (jsonEscapeFunctionResult) {
			currentDateString = JSONObject.escape(currentDateString);
		}
		return timestampMatcher.replaceAll(Matcher.quoteReplacement(currentDateString));
	}

	/**
	 * Replace placeholders in given string value by applying functions
	 * available for this creation log
	 *
	 * @param value the String value in which placeholder are replaced
	 * @return a copy of the initial value with function's placeholders replaced
	 */
	public String processPlaceholdersInString(String value) {
		return processPlaceholders(null, value, false);
	}

	/**
	 * Replace placeholders in given json by applying functions available for
	 * this creation log
	 *
	 * @param json The JSON object onto which functions are applied. Functions
	 *             are applied on the String representation of this object
	 *             (obtained with {@link Json#toString()}). In order to keep the
	 *             string valid with respect to JSON syntax, placeholders are
	 *             unescaped before being processed, and their replacing value is
	 *             escaped before being replaced in the String
	 * @return a copy of the given json object with function's placeholders
	 * replaced
	 * @throws ParseException if the string obtained after placeholder replacement is not a
	 *                        valid JSON object
	 */
	private Object processPlaceholdersInJSON(Json json) throws ParseException {
		return new Json(processPlaceholders(null, json.toString(), true));
	}

	private void getEveryUUIDs(String scenario) {
		Matcher uuidMatcher = UUID_PATTERN.matcher(scenario);
		Map<String, String> uuids = kiteContext.getUuids();
		while (uuidMatcher.find()) {
			String name = uuidMatcher.group(1);
			if (!uuids.containsKey(name)) {
				uuids.put(name, UUID.randomUUID().toString());
			}
		}
	}

	/**
	 * Replace placeholders in given string by applying functions available for
	 * this creation log
	 * <p/>
	 * Also computes UUID corresponding to given command (if not null) and
	 * update the list of UUIDs for this creation log.
	 *
	 * @param commandName              the name of the command for which placeholders are escaped
	 *                                 (context command)
	 * @param valueWithPlaceholders    the String containing the placeholders to replace. This String
	 *                                 may be (or not) the representation of a JSON object (with
	 *                                 escaped values in keys and String values). Thus, placeholders
	 *                                 are json-unescaped before being processed (this have no effect
	 *                                 if this parameter is not a Json String)
	 * @param jsonEscapeFunctionResult if true, function result will be json-escaped before being
	 *                                 replaced in processed string. If false, function return is
	 *                                 injected as is.
	 *                                 <p/>
	 *                                 <b>Use true if the result must be directly parsed as JSON</b>,
	 *                                 false otherwise.
	 * @return the copy of initial valueWithPlaceholders with function's
	 * placeholders replaced
	 */
	public String processPlaceholders(@Nullable String commandName,
									  String valueWithPlaceholders, boolean jsonEscapeFunctionResult) {
		String processedValue = valueWithPlaceholders;

		try {
			// Assign UUID for current command if needed
			if (commandName != null) {
				processedValue = processedValue
						.replace("{{" + UUIDFunction.NAME + "}}", "{{"
								+ UUIDFunction.NAME + ':' + commandName + "}}");
			}
			// Update UUIDs list to add the one assigned for current command
			getEveryUUIDs(processedValue);

			return applyFunctions(processedValue, jsonEscapeFunctionResult);
		} catch (RuntimeException e) {
			throw new IllegalStateException("processPlaceholders : Command [" + commandName + "] failed ", e);
		}
	}

	public Object processPlaceholders(Object expected) throws ParseException {
		if (expected instanceof String) {
			return processPlaceholdersInString((String) expected);
		}

		if (expected instanceof Json) {
			return processPlaceholdersInJSON((Json) expected);
		}

		if (expected instanceof Boolean ||
				expected instanceof Long ||
				expected instanceof Double) {
			return expected;
		}
		throw new UnsupportedOperationException("Incorrect value : " + expected);
	}


	String getProcessedURI(Command command) {
		return processPlaceholders(command.getName(), command.getUri(), false);
	}


	/**
	 * Retrieves the body from command in parameter and process its placeholders.
	 *
	 * @param command the command to get body from, not null
	 * @return the body of the request, with placeholders processed
	 */
	public String getProcessedBody(Command command) {
		if (processedBody != null) {
			return processedBody;
		}

		String body = command.getBody();
		if (Strings.isNullOrEmpty(body)) {
			return "";
		}
		processedBody = processPlaceholders(command.getName(), body, true);
		return processedBody;
	}

	/**
	 * init requestSpecification with the string if request is a string or with a multiPart if request is a file
	 */
	RequestSpecification initRequestSpecificationContent(Command command) {
		RequestSpecification requestSpecification = given();
		Command.MultiPart multiPart = command.getMultiPart();
		String commandName = command.getName();

		if (multiPart == null) {
			String processedBody = getProcessedBody(command);
			if (command.getDebug()) {
				log.info("[{} {}]", commandName, processedBody);
			}
			return requestSpecification.body(processedBody.getBytes(Charsets.UTF_8));
		}


		String fileLocation = processPlaceholders(commandName, multiPart.getFileLocation(), true);
		String multiPartName = processPlaceholders(commandName, multiPart.getName(), true);
		if (command.getDebug()) {
			log.info("[{} , multiPartName:{}, fileLocation:{}]", commandName, multiPartName, fileLocation);
		}
		return requestSpecification.multiPart(
				multiPartName,
				Paths.get(fileLocation).getFileName().toString(),
				FileHelper.getFileInputStream(fileLocation));
	}

	Map<String, String> getProcessedHeaders(Command command) {
		Map<String, String> processedHeaders = new HashMap<>(command.getHeaders());

		for (Map.Entry<String, String> entry : processedHeaders.entrySet()) {
			processedHeaders.put(entry.getKey(),
					processPlaceholders(command.getName(), entry.getValue(), false));
		}

		if (!processedHeaders.containsKey("Accept")) {
			processedHeaders.put("Accept", "application/json");
		}

		return processedHeaders;
	}

	/**
	 * This special version of {@link #getProcessedHeaders(Command)} allow one to use
	 * auth header configured in kite framework during automatic location check.
	 * Kite user must provide a variable for {@link #AUTOMATIC_CHECK_AUTHORIZATION_HEADER_VALUE} value name
	 * and set {@link KiteContext#authorizationHeaderNameForAutomaticCheck}
	 */
	Map<String, String> getProcessedHeadersForCheck(Command command) {
		Map<String, String> processedHeaders = new HashMap<>(command.getHeaders());

		for (Map.Entry<String, String> entry : processedHeaders.entrySet()) {
			processedHeaders.put(entry.getKey(),
					processPlaceholders(command.getName(), entry.getValue(), false));
		}

		if (!processedHeaders.containsKey("Accept")) {
			processedHeaders.put("Accept", "application/json");
		}

		if (kiteContext.getAuthorizationHeaderNameForAutomaticCheck() != null
				&& !processedHeaders.containsKey(kiteContext.getAuthorizationHeaderNameForAutomaticCheck())
				&& kiteContext.getVariables().get(AUTOMATIC_CHECK_AUTHORIZATION_HEADER_VALUE) != null) {
			log.debug("Add authorization header for automatic check");
			processedHeaders.put(kiteContext.getAuthorizationHeaderNameForAutomaticCheck(),
					kiteContext.getVariableValue(AUTOMATIC_CHECK_AUTHORIZATION_HEADER_VALUE));
		}

		return processedHeaders;
	}

	public List<Check> getChecks(Command command) throws ParseException {
		List<Check> checks = new ArrayList<>();
		for (Integer i = 0; i < command.getCommandSpecification().getLength("checks"); ++i) {
			Json json = requireNonNull(command.getCommandSpecification().get("checks"));
			checks.add(new Check(json.get(i), this));
		}

		return checks;
	}
}
