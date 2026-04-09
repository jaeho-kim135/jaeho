package org.knime.bigdata.spark.dx.node;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.widget.choices.StringChoice;
import org.knime.node.parameters.widget.choices.StringChoicesProvider;

/**
 * Provides a sorted list of available locales for dropdown selection in WebUI dialogs.
 * Shared by StringToDateTime, DateTimeToString, and ExtractDateTimeFields nodes.
 */
@SuppressWarnings("restriction")
public final class LocaleChoicesProvider implements StringChoicesProvider {

    @Override
    public List<StringChoice> computeState(final NodeParametersInput context) {
        final Set<StringChoice> choices = new LinkedHashSet<>();
        for (final Locale loc : Locale.getAvailableLocales()) {
            final String tag = loc.toLanguageTag();
            if ("und".equals(tag) || tag.isEmpty()) {
                continue;
            }
            final String display = loc.getDisplayName(Locale.ENGLISH);
            choices.add(new StringChoice(tag, display + " (" + tag + ")"));
        }
        return choices.stream()
            .sorted((a, b) -> a.text().compareToIgnoreCase(b.text()))
            .collect(Collectors.toList());
    }
}
