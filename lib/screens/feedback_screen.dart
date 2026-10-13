import 'dart:convert';

import 'package:flutter/material.dart';

import '../feedback.dart';
import '../theme.dart';
import '../widgets/components.dart';

/// Opens the feedback form. [screenContext] describes what the user was
/// looking at; it is sent with the report along with device details.
void openFeedback(
  BuildContext context, {
  required Map<String, Object?> screenContext,
  FeedbackCategory category = FeedbackCategory.bug,
}) {
  Navigator.push(
    context,
    MaterialPageRoute(
      builder: (_) =>
          FeedbackScreen(screenContext: screenContext, category: category),
    ),
  );
}

/// Trial-period feedback: a bug, wrong event details, an idea, or anything
/// else. Shows the debugging details that will be attached before sending.
class FeedbackScreen extends StatefulWidget {
  final Map<String, Object?> screenContext;
  final FeedbackCategory category;

  const FeedbackScreen({
    super.key,
    required this.screenContext,
    this.category = FeedbackCategory.bug,
  });

  @override
  State<FeedbackScreen> createState() => _FeedbackScreenState();
}

class _FeedbackScreenState extends State<FeedbackScreen> {
  late FeedbackCategory _category = widget.category;
  final _message = TextEditingController();
  final _email = TextEditingController();
  Future<Map<String, Object?>>? _context;
  bool _sending = false;

  @override
  void didChangeDependencies() {
    super.didChangeDependencies();
    // Needs MediaQuery, so not in initState; collected once
    _context ??= collectFeedbackContext(context, widget.screenContext);
  }

  @override
  void dispose() {
    _message.dispose();
    _email.dispose();
    super.dispose();
  }

  Future<void> _send() async {
    final message = _message.text.trim();
    if (message.isEmpty || _sending) return;
    setState(() => _sending = true);
    final messenger = ScaffoldMessenger.of(context);
    try {
      final delivered = await submitFeedback(
        category: _category,
        message: message,
        email: _email.text.trim(),
        context: await _context!,
      );
      messenger.showSnackBar(
        SnackBar(
          content: Text(
            delivered
                ? 'Thanks! Your feedback was sent.'
                : "Thanks! You're offline, so it will be sent when you reconnect.",
          ),
        ),
      );
      if (mounted) Navigator.pop(context);
    } catch (e) {
      debugPrint('Feedback send failed: $e');
      if (!mounted) return;
      setState(() => _sending = false);
      messenger.showSnackBar(
        const SnackBar(content: Text('Could not send. Please try again.')),
      );
    }
  }

  @override
  Widget build(BuildContext context) {
    const categories = FeedbackCategory.values;
    return Scaffold(
      appBar: AppBar(title: const Text('Send feedback')),
      body: ListView(
        padding: const EdgeInsets.fromLTRB(24, 16, 24, 32),
        children: [
          const Text(
            "ToddleGo is in early testing. Tell us what's broken, what's "
            "wrong with an event, or what you'd like next.",
            style: TextStyle(fontSize: 16, color: AppColors.textSecondary),
          ),
          const SizedBox(height: 24),
          // Two rows of two: four in a row do not fit on a 360dp phone
          for (final (i, row) in [
            categories.sublist(0, 2),
            categories.sublist(2),
          ].indexed) ...[
            if (i > 0) const SizedBox(height: 12),
            OutlineChoice<FeedbackCategory>(
              options: [for (final c in row) (c, c.label)],
              selected: _category,
              allowNone: false,
              onSelected: (c) => setState(() => _category = c!),
            ),
          ],
          const SizedBox(height: 24),
          TextField(
            controller: _message,
            minLines: 5,
            maxLines: 10,
            maxLength: 5000,
            textCapitalization: TextCapitalization.sentences,
            onChanged: (_) => setState(() {}),
            decoration: InputDecoration(
              hintText: _category.hint,
              border: const OutlineInputBorder(),
            ),
          ),
          const SizedBox(height: 12),
          TextField(
            controller: _email,
            keyboardType: TextInputType.emailAddress,
            autocorrect: false,
            maxLength: 200,
            decoration: const InputDecoration(
              labelText: 'Your email (optional, for a reply)',
              border: OutlineInputBorder(),
              counterText: '',
            ),
          ),
          const SizedBox(height: 16),
          _AttachedDetails(details: _context),
          const SizedBox(height: 24),
          SizedBox(
            width: double.infinity,
            height: 56,
            child: FilledButton(
              style: FilledButton.styleFrom(
                backgroundColor: AppColors.primary,
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(16),
                ),
              ),
              onPressed: _message.text.trim().isEmpty || _sending
                  ? null
                  : _send,
              child: Text(
                _sending ? 'Sending…' : 'Send',
                style: const TextStyle(fontSize: 18),
              ),
            ),
          ),
        ],
      ),
    );
  }
}

/// Collapsed list of the debugging details sent with the report.
class _AttachedDetails extends StatelessWidget {
  final Future<Map<String, Object?>>? details;

  const _AttachedDetails({required this.details});

  @override
  Widget build(BuildContext context) {
    return Theme(
      // No divider lines around the expansion tile
      data: Theme.of(context).copyWith(dividerColor: Colors.transparent),
      child: ExpansionTile(
        tilePadding: EdgeInsets.zero,
        title: const Text(
          'Sent with your feedback',
          style: TextStyle(fontSize: 16),
        ),
        subtitle: const Text(
          'App version, device, this screen and recent errors, to help us '
          'find the problem. Your location is rounded to about 1 km.',
          style: TextStyle(fontSize: 14, color: AppColors.textSecondary),
        ),
        children: [
          FutureBuilder<Map<String, Object?>>(
            future: details,
            builder: (context, snapshot) => Container(
              width: double.infinity,
              padding: const EdgeInsets.all(12),
              decoration: BoxDecoration(
                color: AppColors.surfaceMuted,
                borderRadius: BorderRadius.circular(12),
              ),
              child: SelectableText(
                snapshot.hasData
                    ? const JsonEncoder.withIndent('  ').convert(snapshot.data)
                    : 'Collecting…',
                style: const TextStyle(fontFamily: 'monospace', fontSize: 12),
              ),
            ),
          ),
        ],
      ),
    );
  }
}
