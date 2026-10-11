import 'package:flutter/material.dart';

/// Colors from the Figma design.
abstract final class AppColors {
  static const primary = Color(0xFF3B82F6);
  static const text = Color(0xFF111827);
  static const textSecondary = Color(0xFF6B7280);
  static const textMuted = Color(0xFF9CA3AF);
  static const surfaceMuted = Color(0xFFF3F4F6);
  static const border = Color(0xFFE5E7EB);
  static const selectedBackground = Color(0xFFEFF6FF);
  static const selectedBorder = Color(0xFF93C5FD);
  static const selectedText = Color(0xFF2563EB);
  static const walkInBackground = Color(0xFFECFDF5);
  static const walkInBorder = Color(0xFFA7F3D0);
  static const walkInText = Color(0xFF059669);
  static const bookBackground = Color(0xFFFFFBEB);
  static const bookBorder = Color(0xFFFCD34D);
  static const bookText = Color(0xFFD97706);
}

ThemeData buildAppTheme() {
  final base = ThemeData(
    colorScheme: ColorScheme.fromSeed(
      seedColor: AppColors.primary,
      primary: AppColors.primary,
      surface: Colors.white,
    ),
    scaffoldBackgroundColor: Colors.white,
    useMaterial3: true,
  );
  return base.copyWith(
    appBarTheme: const AppBarTheme(
      backgroundColor: Colors.white,
      foregroundColor: AppColors.text,
      surfaceTintColor: Colors.white,
      elevation: 0,
      scrolledUnderElevation: 0,
      shape: Border(bottom: BorderSide(color: AppColors.surfaceMuted)),
    ),
    textTheme: base.textTheme.apply(
      bodyColor: AppColors.text,
      displayColor: AppColors.text,
    ),
  );
}
