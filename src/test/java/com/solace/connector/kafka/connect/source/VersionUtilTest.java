package com.solace.connector.kafka.connect.source;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertLinesMatch;

public class VersionUtilTest {
	@Test
	public void testGetVersion() {
		assertLinesMatch(Collections.singletonList(Pattern.compile("^[0-9]+\\.[0-9]+\\.[0-9]+$").pattern()),
				Collections.singletonList(VersionUtil.getVersion()));
	}
}
