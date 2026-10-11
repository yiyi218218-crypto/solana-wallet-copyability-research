export type CopyCaptureAssessment =
  | { status: "UNMEASURED"; reason: string }
  | { status: "MEASURED"; copyCaptureRatio: number; leaderRealizedPnl: number; followerExecutableRealizedPnl: number };

/**
 * Never infer executable performance from historical wallet trades. It is only calculated
 * after timestamped leader events and executable quote/fill evidence exist for the same replay.
 */
export function calculateCopyCaptureRatio(leaderRealizedPnl: number | undefined, followerExecutableRealizedPnl: number | undefined): CopyCaptureAssessment {
  if (leaderRealizedPnl === undefined || followerExecutableRealizedPnl === undefined) return { status: "UNMEASURED", reason: "Executable replay evidence is absent." };
  if (leaderRealizedPnl === 0) return { status: "UNMEASURED", reason: "Leader realized PnL is zero; COPY_CAPTURE_RATIO denominator is undefined." };
  if (leaderRealizedPnl < 0) return { status: "UNMEASURED", reason: "Leader realized PnL is negative; ratio is not a copyability-quality measure." };
  return { status: "MEASURED", copyCaptureRatio: followerExecutableRealizedPnl / leaderRealizedPnl, leaderRealizedPnl, followerExecutableRealizedPnl };
}
