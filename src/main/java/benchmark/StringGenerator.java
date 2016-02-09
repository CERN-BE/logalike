/**
 * Logalike - A stream based message processor
 * Copyright (c) 2015 European Organisation for Nuclear Research (CERN), All Rights Reserved.
 * This software is distributed under the terms of the GNU General Public Licence version 3 (GPL Version 3),
 * copied verbatim in the file “COPYLEFT”.
 * In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue
 * of its status as an Intergovernmental Organization or submit itself to any jurisdiction.
 *
 * Authors: Gergő Horányi <ghoranyi> and Jens Egholm Pedersen <jegp>
 */

package benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Random;

/**
 * Generates random strings.
 */
public class StringGenerator {

    private static final String UTF8_CHARSET = "UTF-8";
    private static final long SEED = 768991312L;
    private static final int UPPER_LINE_LENGTH = 500;
    private static final Random RANDOM = new Random(SEED);

    private StringGenerator() {
        // Should not be instantiated
    }

    /**
     * Generates a {@link String} of random UTF-8 characters.
     *
     * @return A String of length 0 to {@value UPPER_LINE_LENGTH}.
     */
    public static String generateString() {
        final int length = RANDOM.nextInt(UPPER_LINE_LENGTH + 1);
        byte[] array = new byte[length];
        RANDOM.nextBytes(array);
        return new String(array, Charset.forName(UTF8_CHARSET));
    }

}
