package io.salad109.conjunctionapi.conjunction;

import io.salad109.conjunctionapi.satellite.Satellite;

public class PairReduction {

    private PairReduction() {
    }

    /**
     * Determines if two satellites could possibly collide.
     * Applies orbital geometry filters with mathematical certainty.
     */
    public static boolean canCollide(Satellite a, Satellite b, double toleranceKm) {
        // Apply filters starting with computationally cheapest
        return altitudeShellsOverlap(a, b, toleranceKm) &&
                neitherAreDebris(a, b) &&
                orbitalPlanesIntersect(a, b, toleranceKm) &&
                areNotDocked(a, b);
    }

    private static boolean altitudeShellsOverlap(Satellite a, Satellite b, double toleranceKm) {
        double perigeeA = a.getPerigeeKm();
        double apogeeA = a.getApogeeKm();
        double perigeeB = b.getPerigeeKm();
        double apogeeB = b.getApogeeKm();
        return !(apogeeA + toleranceKm < perigeeB || apogeeB + toleranceKm < perigeeA);
    }

    private static boolean neitherAreDebris(Satellite a, Satellite b) {
        return !"DEBRIS".equals(a.getObjectType()) && !"DEBRIS".equals(b.getObjectType());
    }

    private static boolean orbitalPlanesIntersect(Satellite a, Satellite b, double toleranceKm) {
        double iA = Math.toRadians(a.getInclination());
        double iB = Math.toRadians(b.getInclination());
        double raanA = Math.toRadians(a.getRaan());
        double raanB = Math.toRadians(b.getRaan());
        double omegaA = Math.toRadians(a.getArgPerigee());
        double omegaB = Math.toRadians(b.getArgPerigee());
        double eA = a.getEccentricity();
        double eB = b.getEccentricity();
        double aA = a.getSemiMajorAxisKm();
        double aB = b.getSemiMajorAxisKm();

        double deltaRaan = raanA - raanB;

        // Coplanar orbits can intersect anywhere
        double relativeInclination = computeRelativeInclination(iA, iB, deltaRaan);
        if (relativeInclination < Math.toRadians(0.1)) {
            return true;
        }

        // Find where each orbit crosses the intersection line between orbital planes
        double alphaA = computeAlphaA(iA, iB, deltaRaan);
        double alphaB = computeAlphaB(iA, iB, deltaRaan);

        // Convert to true anomaly
        double nuA1 = normalizeAngle(alphaA - omegaA);
        double nuA2 = normalizeAngle(alphaA + Math.PI - omegaA);
        double nuB1 = normalizeAngle(alphaB - omegaB);
        double nuB2 = normalizeAngle(alphaB + Math.PI - omegaB);

        // Compute orbital radius at crossing points
        double rA1 = orbitalRadius(aA, eA, nuA1);
        double rA2 = orbitalRadius(aA, eA, nuA2);
        double rB1 = orbitalRadius(aB, eB, nuB1);
        double rB2 = orbitalRadius(aB, eB, nuB2);

        // If radii match at either crossing point, orbits can intersect
        // Check all four combinations
        double diff1 = Math.abs(rA1 - rB1);
        double diff2 = Math.abs(rA1 - rB2);
        double diff3 = Math.abs(rA2 - rB1);
        double diff4 = Math.abs(rA2 - rB2);

        double minDiff = Math.min(Math.min(diff1, diff2), Math.min(diff3, diff4));
        return minDiff <= toleranceKm;
    }

    private static double computeRelativeInclination(double iA, double iB, double deltaRaan) {
        double cosRelInc = Math.cos(iA) * Math.cos(iB)
                + Math.sin(iA) * Math.sin(iB) * Math.cos(deltaRaan);
        return Math.acos(Math.clamp(cosRelInc, -1, 1));
    }

    private static double computeAlphaA(double iA, double iB, double deltaRaan) {
        double sinIB = Math.sin(iB);
        double cosIB = Math.cos(iB);
        double sinIA = Math.sin(iA);
        double cosIA = Math.cos(iA);
        double sinDeltaRaan = Math.sin(deltaRaan);
        double cosDeltaRaan = Math.cos(deltaRaan);

        double y = sinIB * sinDeltaRaan;
        double x = sinIA * cosIB - cosIA * sinIB * cosDeltaRaan;

        return Math.atan2(y, x);
    }

    private static double computeAlphaB(double iA, double iB, double deltaRaan) {
        double sinIB = Math.sin(iB);
        double cosIB = Math.cos(iB);
        double sinIA = Math.sin(iA);
        double cosIA = Math.cos(iA);
        double sinDeltaRaan = Math.sin(deltaRaan);
        double cosDeltaRaan = Math.cos(deltaRaan);

        double y = -sinIA * sinDeltaRaan;
        double x = sinIB * cosIA - cosIB * sinIA * cosDeltaRaan;

        return Math.atan2(y, x);
    }

    private static double orbitalRadius(double semiMajorAxis, double eccentricity, double trueAnomaly) {
        double p = semiMajorAxis * (1 - eccentricity * eccentricity);
        return p / (1 + eccentricity * Math.cos(trueAnomaly));
    }

    private static double normalizeAngle(double angle) {
        while (angle > Math.PI) angle -= 2 * Math.PI;
        while (angle < -Math.PI) angle += 2 * Math.PI;
        return angle;
    }

    private static boolean areNotDocked(Satellite a, Satellite b) {
        return !(a.getEpoch() != null && a.getEpoch().equals(b.getEpoch()) &&
                a.getMeanMotion() != null && a.getMeanMotion().equals(b.getMeanMotion()) &&
                a.getEccentricity() != null && a.getEccentricity().equals(b.getEccentricity()) &&
                a.getInclination() != null && a.getInclination().equals(b.getInclination()) &&
                a.getRaan() != null && a.getRaan().equals(b.getRaan()) &&
                a.getArgPerigee() != null && a.getArgPerigee().equals(b.getArgPerigee()) &&
                a.getMeanAnomaly() != null && a.getMeanAnomaly().equals(b.getMeanAnomaly()));
    }
}
